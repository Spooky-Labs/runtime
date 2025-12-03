"""
Pub/Sub Market Data Feed for Backtrader

This module connects Backtrader to Google Cloud Pub/Sub to receive real-time
market data. It's the "input layer" that streams live prices into the trading
engine for the agent to make decisions on.

Data Flow:
    1. Data Ingestor publishes OHLCV bars to Pub/Sub topics
    2. SharedSubscriber manages ONE subscription per topic (not per symbol!)
    3. SharedSubscriber routes messages to the correct feed by symbol
    4. Messages are buffered in a lock-free deque
    5. Backtrader pulls from buffer bar-by-bar

Why SharedSubscriber Pattern:
- Pub/Sub has 10,000 subscription limit per topic/project
- Old pattern: 70 subscriptions per agent → limit reached at 140 agents
- New pattern: 2 subscriptions per agent → supports 5,000 agents

Key Design Decisions:
1. **Lock-Free Buffering**: Using deque with maxlen for efficient, thread-safe
   buffering without explicit locks.

2. **Shared Subscriptions**: All feeds for the same topic share ONE subscription
   via SharedSubscriber. Messages are routed by symbol to correct feed buffer.

3. **Content-Addressed Hashing**: Each data point includes a hash from the
   ingestor, enabling FMEL to correlate decisions with exact data versions.
"""

import backtrader as bt
import json
import logging
from collections import deque
from datetime import datetime, timezone

from shared_subscriber import SharedSubscriber

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
        ('agent_id', None),  # Agent ID for subscription naming (Firebase agent ID)
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
        # Note: SharedSubscriber routes messages directly into this buffer.
        self._data_buffer = deque(maxlen=self.p.buffer_size)

        # Reference to the shared subscriber (set in start())
        self._shared_subscriber = None

        # Hash of current data point - used by FMEL for traceability.
        # This lets us correlate decisions with exact data versions.
        self.current_data_hash = None

        # Monitoring stats
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
        Start the data feed by registering with the SharedSubscriber.

        The SharedSubscriber manages ONE subscription per topic, shared across
        all feeds. This dramatically reduces subscription count from O(symbols)
        to O(topics) per agent.

        Flow:
        1. Get SharedSubscriber singleton for this topic
        2. Register this feed to receive messages for our symbol
        3. SharedSubscriber routes messages to our _data_buffer
        """
        if self._running:
            return

        try:
            # Get or create the shared subscriber for this topic
            self._shared_subscriber = SharedSubscriber.get_instance(
                self.p.project_id,
                self.p.topic_name,
                self.p.agent_id
            )

            # Register this feed to receive messages for our symbol
            self._shared_subscriber.register_feed(self.p.symbol, self)

            self._running = True
            logger.info(f"Feed started for {self.p.symbol} (shared subscription)")

        except Exception as e:
            logger.error(f"Failed to start feed for {self.p.symbol}: {e}")
            raise

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

        # Unregister from shared subscriber
        # Note: SharedSubscriber.stop() is called by runner.py cleanup,
        # which deletes the actual subscription.
        if self._shared_subscriber:
            try:
                self._shared_subscriber.unregister_feed(self.p.symbol)
            except Exception as e:
                logger.debug(f"Feed unregistration: {e}")

        # Log final stats
        logger.info(
            f"Feed {self.p.symbol} stopped - "
            f"Processed: {self._messages_processed}, "
            f"Buffered: {len(self._data_buffer)}"
        )

    def __del__(self):
        """Cleanup on deletion"""
        self.stop()