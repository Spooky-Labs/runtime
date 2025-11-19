"""
Pub/Sub Market Data Feed for Backtrader

This module connects Backtrader to Google Cloud Pub/Sub to receive real-time
market data. It's the "input layer" that streams live prices into the trading
engine for the agent to make decisions on.

Data Flow:
    1. Data Ingestor publishes OHLCV bars to Pub/Sub topics
    2. This feed creates a filtered subscription for its symbol
    3. Messages are buffered in a lock-free deque
    4. Backtrader pulls from buffer bar-by-bar

Why Pub/Sub:
- Decouples data ingestion from trading agents
- Multiple agents can consume the same data stream
- Automatic message retention and replay capability
- Scales to thousands of symbols

Key Design Decisions:
1. **Lock-Free Buffering**: Using deque with maxlen for efficient, thread-safe
   buffering without explicit locks.

2. **Filtered Subscriptions**: Each feed creates its own subscription with a
   filter for its specific symbol. This prevents agents from receiving data
   for symbols they don't trade.

3. **Content-Addressed Hashing**: Each data point includes a hash from the
   ingestor, enabling FMEL to correlate decisions with exact data versions.
"""

import backtrader as bt
import json
import logging
from collections import deque
from datetime import datetime, timezone
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


class PubSubMarketDataFeed(bt.feeds.DataBase):
    """
    Backtrader data feed that streams live market data from Google Pub/Sub.

    This feed creates a Pub/Sub subscription, receives OHLCV bars, and converts
    them into Backtrader's internal format. Each bar becomes one "tick" that
    the trading strategy's next() method processes.

    State Lifecycle:
        DELAYED → LIVE → DISCONNECTED
        - Starts in DELAYED while waiting for first message
        - Transitions to LIVE when first message arrives
        - Goes to DISCONNECTED on stop or error
    """

    # Standard OHLCV lines that Backtrader expects.
    lines = ('open', 'high', 'low', 'close', 'volume')

    params = (
        ('project_id', None),  # GCP project ID
        ('topic_name', None),  # Pub/Sub topic (crypto-data or market-data)
        ('symbol', None),  # Symbol to subscribe to (e.g., BTC/USD)
        ('buffer_size', 1000),  # Max messages to buffer before dropping old ones
    )

    # States for feed lifecycle
    DELAYED = 0
    LIVE = 1
    DISCONNECTED = 2

    def __init__(self):
        super().__init__()

        # =================================================================
        # STATE MANAGEMENT
        # =================================================================
        self._state = self.DELAYED  # Current feed state
        self._laststatus = self.DELAYED  # Last status reported to Backtrader
        self._running = False

        # Lock-free buffer for incoming messages.
        # Using maxlen automatically discards oldest messages if we fall behind.
        self._data_buffer = deque(maxlen=self.p.buffer_size)

        # =================================================================
        # PUB/SUB COMPONENTS
        # =================================================================
        self._subscriber = None  # SubscriberClient instance
        self._subscription = None  # Active subscription future
        self._subscription_path = None  # Full path to subscription resource

        # Hash of current data point - used by FMEL for traceability.
        # This lets us correlate decisions with exact data versions.
        self.current_data_hash = None

        # Monitoring stats
        self._messages_received = 0
        self._messages_processed = 0

        logger.info(f"Data feed initialized for {self.p.symbol}")

    def islive(self):
        """Indicate this is a live data feed"""
        return True

    def haslivedata(self):
        """Check if live data is available"""
        return len(self._data_buffer) > 0

    def start(self):
        """
        Start the data feed by creating a Pub/Sub subscription.

        This creates a unique subscription filtered by symbol, ensuring we only
        receive data for the symbol we're trading. The subscription is deleted
        on stop() to avoid orphaned resources.

        Flow:
        1. Create SubscriberClient
        2. Build unique subscription name (includes object ID to avoid collisions)
        3. Create subscription with symbol filter
        4. Start async message pulling
        """
        if self._running:
            return

        try:
            self._subscriber = pubsub_v1.SubscriberClient()

            # Build unique subscription name.
            # Format: feed-BTC-USD-7f8a3b (symbol + object ID hex)
            topic_path = f"projects/{self.p.project_id}/topics/{self.p.topic_name}"
            subscription_id = f"feed-{self.p.symbol.replace('/', '-')}-{id(self):x}"
            self._subscription_path = f"projects/{self.p.project_id}/subscriptions/{subscription_id}"

            # Create subscription with filter for this symbol only.
            self._create_subscription(topic_path)

            # Start async message pulling with flow control.
            flow_control = pubsub_v1.types.FlowControl(max_messages=self.p.buffer_size)
            self._subscription = self._subscriber.subscribe(
                self._subscription_path,
                callback=self._handle_message,
                flow_control=flow_control
            )

            self._running = True
            logger.info(f"Feed started for {self.p.symbol} [{subscription_id}]")

        except Exception as e:
            logger.error(f"Failed to start feed for {self.p.symbol}: {e}")
            raise

    def _create_subscription(self, topic_path: str):
        """Create subscription with idempotent retry"""
        try:
            # Try to delete existing subscription
            self._subscriber.delete_subscription(
                request={"subscription": self._subscription_path}
            )
        except Exception:
            pass  # Subscription didn't exist

        # Create new subscription with filter
        self._subscriber.create_subscription(
            request={
                "name": self._subscription_path,
                "topic": topic_path,
                "filter": f'attributes.symbol = "{self.p.symbol}"',
                "ack_deadline_seconds": 20,
                "enable_message_ordering": True if "/" not in self.p.symbol else False,
            }
        )

    def _handle_message(self, message: pubsub_v1.subscriber.message.Message):
        """Efficiently handle incoming messages"""
        try:
            # Parse message
            data = json.loads(message.data.decode('utf-8'))

            # Add to buffer
            self._data_buffer.append(data)
            self._messages_received += 1

            # Transition to LIVE on first message
            if self._state == self.DELAYED:
                self._state = self.LIVE
                self._laststatus = self.LIVE  # Update Backtrader status tracking
                self.put_notification(self.LIVE)
                logger.info(f"Feed {self.p.symbol}: DELAYED → LIVE")

            # Acknowledge immediately
            message.ack()

            # Log periodically
            if self._messages_received % 100 == 0:
                logger.debug(f"Feed {self.p.symbol}: {self._messages_received} messages received")

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message: {e}")
            message.nack()
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            message.nack()

    def _load(self):
        """Load next data point with minimal latency"""
        if not self._running:
            return False

        if not self._data_buffer:
            return None  # Waiting for data

        try:
            # Pop oldest message (FIFO)
            data = self._data_buffer.popleft()
            self._messages_processed += 1

            # Parse timestamp efficiently
            timestamp_str = data.get('timestamp', '')
            if timestamp_str:
                # Handle both 'Z' and '+00:00' formats
                if timestamp_str.endswith('Z'):
                    timestamp_str = timestamp_str[:-1] + '+00:00'
                dt = datetime.fromisoformat(timestamp_str)
            else:
                dt = datetime.now(timezone.utc)

            # Set OHLCV lines - this is what Backtrader uses for charting and indicators.
            self.lines.datetime[0] = bt.date2num(dt)
            self.lines.open[0] = float(data.get('open', 0))
            self.lines.high[0] = float(data.get('high', 0))
            self.lines.low[0] = float(data.get('low', 0))
            self.lines.close[0] = float(data.get('close', 0))
            self.lines.volume[0] = float(data.get('volume', 0))

            # Store data hash for FMEL traceability.
            # This lets us correlate trading decisions with exact data versions.
            self.current_data_hash = data.get('data_hash')

            return True

        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Invalid data format: {e}")
            return None  # Skip bad data
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return False  # Fatal error

    def stop(self):
        """Gracefully stop the data feed"""
        if not self._running:
            return

        logger.info(f"Stopping feed for {self.p.symbol}")
        self._running = False
        self._state = self.DISCONNECTED
        self._laststatus = self.DISCONNECTED

        # Cancel subscription
        if self._subscription:
            try:
                self._subscription.cancel()
                self._subscription.result(timeout=5)  # Wait for cancellation
            except Exception as e:
                logger.debug(f"Subscription cancellation: {e}")

        # Delete subscription
        if self._subscriber and self._subscription_path:
            try:
                self._subscriber.delete_subscription(
                    request={"subscription": self._subscription_path}
                )
            except Exception as e:
                logger.debug(f"Subscription deletion: {e}")

        # Close subscriber
        if self._subscriber:
            try:
                self._subscriber.close()
            except Exception as e:
                logger.debug(f"Subscriber close: {e}")

        # Log final stats
        logger.info(
            f"Feed {self.p.symbol} stopped - "
            f"Received: {self._messages_received}, "
            f"Processed: {self._messages_processed}, "
            f"Buffered: {len(self._data_buffer)}"
        )

    def __del__(self):
        """Cleanup on deletion"""
        self.stop()