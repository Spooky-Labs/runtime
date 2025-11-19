"""
Alpaca Paper Trading Broker for Backtrader

This module bridges Backtrader's broker interface with Alpaca's Broker API,
enabling paper trading execution in the runtime. It serves as the "execution layer"
that translates the agent's buy/sell decisions into actual (simulated) orders.

Why we need this:
- Backtrader has its own broker interface with methods like buy(), sell(), getcash()
- Alpaca has a REST API for order submission and account queries
- This broker translates between the two, maintaining state consistency

Key Design Decisions:
1. **Async Order Monitoring**: Orders are tracked via a background thread that
   polls Alpaca every 2 seconds. This prevents blocking the main trading loop.

2. **Thread-Safe Notifications**: Order updates go through a queue to safely
   notify Backtrader of state changes from the monitoring thread.

3. **Broker API vs Trading API**: We use BrokerClient (not TradingClient) because
   our platform manages multiple user accounts. The Broker API allows us to
   operate on behalf of users' paper trading accounts.

4. **Sandbox Mode**: Always enabled - this is a paper trading platform.
   No real money is involved.
"""

import backtrader as bt
import logging
import threading
import queue
from collections import defaultdict
from typing import Dict, Any
from alpaca.broker.client import BrokerClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus

logger = logging.getLogger(__name__)


class AlpacaPaperTradingBroker(bt.BrokerBase):
    """
    Backtrader broker implementation backed by Alpaca's Broker API.

    This class implements Backtrader's BrokerBase interface, allowing the
    trading strategy to use standard bt.buy() and bt.sell() calls while
    actual execution happens through Alpaca's paper trading system.

    Order Lifecycle:
        1. Agent calls self.buy() or self.sell()
        2. This broker submits order to Alpaca API
        3. Background thread monitors order status
        4. When filled, notification is queued
        5. Backtrader processes notification
        6. Portfolio state is refreshed from Alpaca
    """

    params = (
        ('api_key', None),
        ('secret_key', None),
        ('account_id', None),  # UUID of the paper trading account
        ('sandbox', True),  # Always True - paper trading only
        ('poll_interval', 2.0),  # How often to check order status (seconds)
    )

    # Maps Alpaca order status to Backtrader order method names.
    # When an order reaches one of these states, we call the corresponding
    # method on the Backtrader order object to update its status.
    STATUS_MAP = {
        OrderStatus.FILLED: 'completed',  # Order fully executed
        OrderStatus.PARTIALLY_FILLED: 'partial',  # Some shares filled
        OrderStatus.ACCEPTED: 'accept',  # Order accepted by exchange
        OrderStatus.CANCELED: 'cancel',  # User or system canceled
        OrderStatus.REJECTED: 'reject',  # Exchange rejected
        OrderStatus.EXPIRED: 'expire',  # Time-based expiration
    }

    def __init__(self):
        super().__init__()

        # Validate that we have all required credentials.
        if not all([self.p.api_key, self.p.secret_key, self.p.account_id]):
            raise ValueError("API key, secret key, and account ID required")

        # Initialize Alpaca Broker API client.
        # We use BrokerClient (not TradingClient) because we're operating on
        # behalf of user accounts, not a single trading account.
        self.client = BrokerClient(
            api_key=self.p.api_key,
            secret_key=self.p.secret_key,
            sandbox=self.p.sandbox  # Always True for paper trading
        )

        # =====================================================================
        # PORTFOLIO STATE
        # Cached locally to avoid API calls on every getcash()/getvalue() call.
        # Updated when orders are filled via refresh_account().
        # =====================================================================
        self._cash = 0.0
        self._value = 0.0
        self._positions = defaultdict(lambda: {'size': 0, 'price': 0.0})

        # =====================================================================
        # ORDER TRACKING
        # We maintain bidirectional mappings between Backtrader orders and
        # Alpaca orders to handle status updates from either direction.
        # =====================================================================
        self._orders = {}  # order.ref → {'alpaca_id': str, 'order': bt.Order}
        self._alpaca_to_bt = {}  # alpaca_id → order.ref (reverse lookup)

        # Thread-safe queue for notifications from background thread to main loop.
        self._notifications = queue.Queue()

        # =====================================================================
        # BACKGROUND ORDER MONITORING
        # Alpaca doesn't push order updates to us - we have to poll.
        # The monitoring thread checks order status every poll_interval seconds.
        # =====================================================================
        self._monitor_thread = None
        self._stop_monitoring = threading.Event()

        # Load initial account state and start monitoring.
        self.refresh_account()
        self._start_order_monitoring()

        logger.info(f"Broker initialized - Cash: ${self._cash:,.2f}, Equity: ${self._value:,.2f}")

    def _start_order_monitoring(self):
        """Start background thread for order status monitoring."""
        # daemon=True ensures thread dies when main process exits.
        self._monitor_thread = threading.Thread(target=self._monitor_orders, daemon=True)
        self._monitor_thread.start()

    def _monitor_orders(self):
        """
        Background thread that polls Alpaca for order status updates.

        This is necessary because Alpaca doesn't push order updates to us.
        The thread runs every poll_interval seconds and checks if any
        tracked orders have changed state (e.g., filled, canceled).
        """
        while not self._stop_monitoring.is_set():
            if self._orders:  # Only poll if we have pending orders
                self._update_order_statuses()
            # Wait for poll_interval, but wake up immediately if stop is requested.
            self._stop_monitoring.wait(self.p.poll_interval)

    def _update_order_statuses(self):
        """Batch update order statuses"""
        try:
            # Get all open orders from Alpaca
            open_orders = self.client.get_orders_for_account(
                account_id=self.p.account_id,
                filter={'status': 'open'}
            )

            # Create set of open order IDs
            open_ids = {order.id for order in open_orders}

            # Check each tracked order
            for order_ref, info in list(self._orders.items()):
                alpaca_id = info['alpaca_id']
                bt_order = info['order']

                if alpaca_id not in open_ids:
                    # Order is no longer open, get final status
                    try:
                        alpaca_order = self.client.get_order_for_account(
                            account_id=self.p.account_id,
                            order_id=alpaca_id
                        )
                        self._process_order_update(order_ref, alpaca_order, bt_order)
                    except Exception as e:
                        logger.debug(f"Error fetching order {alpaca_id}: {e}")

        except Exception as e:
            logger.error(f"Error monitoring orders: {e}")

    def _process_order_update(self, order_ref: int, alpaca_order: Any, bt_order: bt.Order):
        """Process an order status update"""
        if alpaca_order.status in self.STATUS_MAP:
            method = self.STATUS_MAP[alpaca_order.status]

            # Update Backtrader order
            getattr(bt_order, method)()

            # Queue notification
            self._notifications.put((bt_order, alpaca_order.status))

            # Remove from tracking if terminal state
            if alpaca_order.status in {OrderStatus.FILLED, OrderStatus.CANCELED,
                                      OrderStatus.REJECTED, OrderStatus.EXPIRED}:
                del self._orders[order_ref]
                del self._alpaca_to_bt[alpaca_order.id]

                # Refresh account on fill
                if alpaca_order.status == OrderStatus.FILLED:
                    self.refresh_account()

                logger.info(f"Order {alpaca_order.status.name}: {alpaca_order.symbol} "
                          f"[{alpaca_order.id[:8]}]")

    def refresh_account(self):
        """Efficiently refresh account state"""
        try:
            # Get account and positions in one batch
            account = self.client.get_trade_account_by_id(account_id=self.p.account_id)
            self._cash = float(account.cash)
            self._value = float(account.equity)

            # Update positions
            positions = self.client.get_all_positions_for_account(account_id=self.p.account_id)
            self._positions.clear()
            for p in positions:
                self._positions[p.symbol] = {
                    'size': float(p.qty),
                    'price': float(p.avg_entry_price)
                }
            return True
        except Exception as e:
            logger.error(f"Account refresh failed: {e}")
            return False

    def getcash(self) -> float:
        """Get available cash"""
        return self._cash

    def getvalue(self) -> float:
        """Get total portfolio value"""
        return self._value

    def getposition(self, data) -> bt.Position:
        """Get position for a data feed"""
        pos = self._positions.get(data._name, {'size': 0, 'price': 0})
        return bt.Position(size=pos['size'], price=pos['price'])

    def buy(self, owner, data, size, price=None, exectype=None, **kwargs):
        """Submit buy order"""
        return self._submit_order(owner, data, size, price, OrderSide.BUY, exectype)

    def sell(self, owner, data, size, price=None, exectype=None, **kwargs):
        """Submit sell order"""
        return self._submit_order(owner, data, size, price, OrderSide.SELL, exectype)

    def _submit_order(self, owner, data, size, price, side, exectype):
        """
        Submit an order to Alpaca and track it for status updates.

        This is the core order execution logic. It:
        1. Creates a Backtrader order object for internal tracking
        2. Builds an Alpaca order request (market or limit)
        3. Submits to Alpaca API
        4. Sets up tracking so we can correlate status updates

        Args:
            owner: The strategy that placed the order
            data: The data feed (symbol) to trade
            size: Number of shares/units to trade
            price: Limit price (None for market orders)
            side: OrderSide.BUY or OrderSide.SELL
            exectype: Order type (Market, Limit, etc.)

        Returns:
            bt.Order: The Backtrader order object (may be rejected if submission fails)
        """
        symbol = data._name

        # Create Backtrader order object first so we can reject it if API fails.
        OrderClass = bt.BuyOrder if side == OrderSide.BUY else bt.SellOrder
        order = OrderClass(
            owner=owner,
            data=data,
            size=size,
            price=price,
            exectype=exectype or bt.Order.Market
        )

        try:
            # Determine if this is a market or limit order.
            is_market = price is None or exectype == bt.Order.Market

            # Build Alpaca order request. GTC = Good Till Canceled.
            if is_market:
                request = MarketOrderRequest(
                    symbol=symbol,
                    qty=size,
                    side=side,
                    time_in_force=TimeInForce.GTC
                )
            else:
                request = LimitOrderRequest(
                    symbol=symbol,
                    qty=size,
                    side=side,
                    limit_price=price,
                    time_in_force=TimeInForce.GTC
                )

            # Submit to Alpaca Broker API.
            alpaca_order = self.client.submit_order_for_account(
                account_id=self.p.account_id,
                order_data=request
            )

            # Set up tracking for background monitoring.
            self._orders[order.ref] = {
                'alpaca_id': alpaca_order.id,
                'order': order
            }
            self._alpaca_to_bt[alpaca_order.id] = order.ref

            # Mark order as submitted and accepted in Backtrader.
            order.submit()
            order.accept()

            order_type = "MKT" if is_market else f"LMT@{price:.2f}"
            logger.info(f"{side.name} {order_type}: {symbol} x{size} [{alpaca_order.id[:8]}]")

        except Exception as e:
            # If API call fails, reject the order so strategy knows it failed.
            logger.error(f"Order submission failed: {e}")
            order.reject()

        return order

    def cancel(self, order):
        """Cancel an order elegantly"""
        if order.ref not in self._orders:
            return False

        try:
            info = self._orders[order.ref]
            self.client.cancel_order_for_account(
                account_id=self.p.account_id,
                order_id=info['alpaca_id']
            )

            # Clean up tracking
            del self._orders[order.ref]
            del self._alpaca_to_bt[info['alpaca_id']]

            order.cancel()
            logger.info(f"Order canceled: {info['alpaca_id'][:8]}")
            return True

        except Exception as e:
            logger.error(f"Cancel failed: {e}")
            return False

    def get_notification(self):
        """Get next notification from queue"""
        try:
            return self._notifications.get_nowait()
        except queue.Empty:
            return None

    def stop(self):
        """Gracefully stop the broker"""
        self._stop_monitoring.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        logger.info("Broker stopped")

    def __del__(self):
        """Cleanup on deletion"""
        self.stop()