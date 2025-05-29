import backtrader as bt
import logging
from alpaca.broker.client import BrokerClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus

logger = logging.getLogger(__name__)

class AlpacaPaperTradingBroker(bt.BrokerBase):
    """
    Broker implementation for Alpaca Paper Trading using the Broker API.
    For broker-dealers managing multiple accounts.
    """
    
    params = (
        ('api_key', None),          # Alpaca API key
        ('secret_key', None),       # Alpaca API secret 
        ('account_id', None),       # Alpaca account ID (required for broker API)
        ('sandbox', True),          # Use sandbox environment by default
    )
    
    def __init__(self):
        # Call the parent constructor first
        bt.BrokerBase.__init__(self)
        
        # Initialize state
        self._cash = 0
        self._value = 0
        self._orders = {}  # backtrader order ref -> alpaca order
        self._positions = {}
        
        # Add startingcash attribute needed by Backtrader's writer
        self.startingcash = 0
        
        # Initialize Alpaca client
        if not self.p.api_key or not self.p.secret_key:
            raise ValueError("API key and secret key must be provided")
            
        # Use BrokerClient for broker-dealer operations
        self.client = BrokerClient(
            api_key=self.p.api_key,
            secret_key=self.p.secret_key,
            sandbox=self.p.sandbox
        )
        
        logger.info("Alpaca Broker client initialized")
        
        # Validate account_id
        if not self.p.account_id:
            raise ValueError("Account ID must be provided for Broker API")
            
        # Get initial account state
        self.refresh_account()
        
        # Set starting cash after initial refresh
        self.startingcash = self._cash

        self.notifications = []

    def get_notification(self):
        if self.notifications:
            return self.notifications.pop(0)
        return None

    def notify_order_event(self, order_event):
        self.notifications.append(order_event)
    
    def refresh_account(self):
        """Fetch latest account information from Alpaca"""
        try:
            # Get account details using get_account_by_id method
            account = self.client.get_account_by_id(account_id=self.p.account_id)
            
            # Inspect account object for debugging
            if hasattr(account, '_raw'):
                # Account object might have raw data accessible via _raw attribute
                account_data = account._raw
                logger.debug(f"Account raw data keys: {account_data.keys() if hasattr(account_data, 'keys') else 'No keys'}")
                
                # Try to get cash from raw data
                if isinstance(account_data, dict):
                    self._cash = float(account_data.get('cash', 0))
                    self._value = float(account_data.get('equity', 0))
                else:
                    # Fall back to direct attribute access
                    self._cash = float(getattr(account, 'cash', 0))
                    self._value = float(getattr(account, 'equity', 0))
            else:
                # Try direct attribute access
                self._cash = float(getattr(account, 'cash', 0))
                self._value = float(getattr(account, 'equity', 0))
                
                # If still zero, check other possible attribute names
                if self._cash == 0:
                    if hasattr(account, 'buying_power'):
                        self._cash = float(account.buying_power) / 2  # Buying power is typically 2x cash
                    elif hasattr(account, 'last_equity'):
                        self._cash = float(account.last_equity)
                
                if self._value == 0 and hasattr(account, 'last_equity'):
                    self._value = float(account.last_equity)
            
            logger.info(f"Account refreshed - Cash: ${self._cash:.2f}, Equity: ${self._value:.2f}")
            
            # Fetch current positions using get_all_positions_for_account method
            positions = self.client.get_all_positions_for_account(account_id=self.p.account_id)
            self._positions = {
                p.symbol: {
                    'size': float(p.qty), 
                    'price': float(p.avg_entry_price)
                } for p in positions
            }
            
            logger.info(f"Positions refreshed - {len(self._positions)} active positions")
            return True
        except Exception as e:
            logger.error(f"Error refreshing account: {e}")
            return False
    
    def getcash(self):
        """Return available cash"""
        return self._cash
    
    def getvalue(self):
        """Return total portfolio value"""
        return self._value
    
    def getposition(self, data):
        """Get position for the given asset"""
        symbol = data._name
        position = self._positions.get(symbol, {'size': 0, 'price': 0})
        return bt.Position(position['size'], position['price'])
    
    def buy(self, owner, data, size, price=None, exectype=None, **kwargs):
        """Create a buy order"""
        logger.info(f"Creating buy order: {data._name}, size={size}, price={price}")
        order = self._create_order(
            owner=owner, 
            data=data, 
            size=size,
            price=price,
            side=OrderSide.BUY,
            exectype=exectype,
            **kwargs
        )
        return order
    
    def sell(self, owner, data, size, price=None, exectype=None, **kwargs):
        """Create a sell order"""
        logger.info(f"Creating sell order: {data._name}, size={size}, price={price}")
        order = self._create_order(
            owner=owner, 
            data=data, 
            size=size,
            price=price,
            side=OrderSide.SELL,
            exectype=exectype,
            **kwargs
        )
        return order
    
    def _create_order(self, owner, data, size, price, side, exectype=None, **kwargs):
        """Unified order creation logic"""
        # Create Backtrader order
        order = self._create_bt_order(owner, data, size, price, side, exectype)
        
        # Determine order type
        is_market = price is None or exectype is None or exectype == bt.Order.Market
        
        try:
            if is_market:
                # Create market order
                order_request = MarketOrderRequest(
                    symbol=data._name,
                    qty=size,
                    side=side,
                    time_in_force=TimeInForce.DAY
                )
            else:
                # Create limit order
                order_request = LimitOrderRequest(
                    symbol=data._name,
                    qty=size,
                    side=side,
                    limit_price=price,
                    time_in_force=TimeInForce.DAY
                )
            
            # Submit order to Alpaca using the broker submit_order_for_account method
            alpaca_order = self.client.submit_order_for_account(
                account_id=self.p.account_id,
                order_data=order_request
            )
            
            # Store order reference
            self._orders[order.ref] = alpaca_order.id
            logger.info(f"Order submitted: {alpaca_order.id} ({data._name}, {side}, {size})")
            
            # Always refresh account after order submission
            self.refresh_account()
            
        except Exception as e:
            logger.error(f"Order submission error: {e}")
            order.reject()
        
        return order
    
    def _create_bt_order(self, owner, data, size, price, side, exectype):
        """Create the appropriate Backtrader order object"""
        order_type = bt.Order.Market if exectype is None else exectype
        
        if side == OrderSide.BUY:
            return bt.BuyOrder(owner=owner, data=data, size=size, 
                             price=price, exectype=order_type)
        else:
            return bt.SellOrder(owner=owner, data=data, size=size, 
                              price=price, exectype=order_type)
    
    def cancel(self, order):
        """Cancel an order"""
        if order.ref not in self._orders:
            return False
        
        try:
            alpaca_order_id = self._orders[order.ref]
            # Use the broker cancel_order_for_account method
            self.client.cancel_order_for_account(
                account_id=self.p.account_id,
                order_id=alpaca_order_id
            )
            order.cancel()
            self.refresh_account()
            return True
        except Exception as e:
            logger.error(f"Cancel order error: {e}")
            return False
    
    def notify(self):
        """Check status of all pending orders"""
        for order_ref, alpaca_id in list(self._orders.items()):
            try:
                # Use the broker get_order_for_account method
                alpaca_order = self.client.get_order_for_account(
                    account_id=self.p.account_id,
                    order_id=alpaca_id
                )
                
                # Update backtrader order status based on Alpaca status
                for order in self.orders:
                    if order.ref == order_ref:
                        if alpaca_order.status == OrderStatus.FILLED:
                            order.completed()
                            # Clean up after completion
                            del self._orders[order_ref]
                        elif alpaca_order.status == OrderStatus.CANCELED:
                            order.cancel()
                            del self._orders[order_ref]
                        elif alpaca_order.status == OrderStatus.REJECTED:
                            order.reject()
                            del self._orders[order_ref]
                        
            except Exception as e:
                logger.error(f"Error checking order {alpaca_id}: {e}")