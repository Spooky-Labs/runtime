import backtrader as bt
import json
import queue
import logging
from datetime import datetime, timezone
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)

class PubSubMarketDataFeed(bt.feeds.DataBase):
    """
    Simplified data feed that consumes market data from Google Cloud Pub/Sub.
    Focuses on core OHLCV data for reliable operation.
    """
    
    # Define the lines
    lines = ('open', 'high', 'low', 'close', 'volume')
    
    # Define parameters
    params = (
        ('project_id', None),     # GCP project ID
        ('topic_name', None),     # Pub/Sub topic name
        ('symbol', None),         # Symbol to filter messages
    )
    
    def __init__(self):
        # Call parent class constructor
        super(PubSubMarketDataFeed, self).__init__()
        
        # For Live Trading
        self._laststatus = self.DELAYED
        self._state = self.DELAYED

        # Data buffer for incoming messages
        self._data_buffer = queue.Queue()
        self._subscription = None
        self._subscriber = None
        self._subscription_path = None
        self._running = False
        
        logger.info(f"Initialized PubSub data feed for {self.p.symbol}")

    def islive(self):
        return True
    
    def haslivedata(self):
        return True
    
    def start(self):
        """Set up Pub/Sub subscription and start receiving data"""
        if self._running:
            return
        
        # Create a subscriber client
        self._subscriber = pubsub_v1.SubscriberClient()
        # Define topic and subscription paths
        topic_path = f"projects/{self.p.project_id}/topics/{self.p.topic_name}"
        # Create a unique subscription for this feed instance
        subscription_id = f"feed-{self.p.symbol.replace("/", "-")}-{id(self):x}"
        self._subscription_path = f"projects/{self.p.project_id}/subscriptions/{subscription_id}"

        self._subscriber.create_subscription(
            request={
                "name": self._subscription_path,
                "topic": topic_path,
                "filter": f"attributes.symbol = \"{self.p.symbol.replace("/", "-")}\""
            }
        )
        logger.info(f"Created subscription: {self._subscription_path}")

        # Define the callback function for handling messages
        def handle_message(message):
            try:
                # Decode message data
                logger.info(f"[HANDLE_MESSAGE] Received for {self.p.symbol}: {message.data}")
                data_str = message.data.decode('utf-8')
                data = json.loads(data_str)
                # Add to buffer
                self._data_buffer.put(data)

                # Mark feed as LIVE if it wasn't already
                if self._state != self.LIVE:
                    logger.info(f"Changing State from: {self._state} to {self.LIVE}")
                    self._state = self.LIVE
                    self.put_notification(self._state)

                # Acknowledge the message
                message.ack()
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                message.nack()
            
        # Start the subscription
        logger.info(f"Starting subscription for {self.p.symbol}")
        self._subscription = self._subscriber.subscribe(
            self._subscription_path, callback=handle_message
        )
        
        self._running = True
        logger.info(f"Data feed started for {self.p.symbol}")
    
    def stop(self):
        """Clean up resources"""
        if not self._running:
            return
            
        logger.info(f"Stopping data feed for {self.p.symbol}")
        
        # Cancel subscription
        if self._subscription:
            self._subscription.cancel()
            self._subscription = None
        
        # Delete the subscription
        if self._subscriber and self._subscription_path:
            try:
                self._subscriber.delete_subscription(
                    request={"subscription": self._subscription_path}
                )
                logger.info(f"Deleted subscription: {self._subscription_path}")
            except Exception as e:
                logger.warning(f"Failed to delete subscription: {e}")
        
        # Close subscriber client
        if self._subscriber:
            self._subscriber.close()
            self._subscriber = None
            
        self._running = False
        logger.info(f"Data feed stopped for {self.p.symbol}")
    
    def _load(self):
        """Process incoming market data."""
        if not self._running:
            logger.warning(f"Data feed not running for {self.p.symbol}")
            return False
            
        if self._data_buffer.empty():
            return None  # Critical: Return None, not False when waiting for data
        
        try:
            # Get next message from buffer
            data = self._data_buffer.get(block=False)
            # Update datetime (required by Backtrader)
            if 'timestamp' in data:
                try:
                    # Parse timestamp, assuming ISO format with potential timezone info
                    timestamp_str = data['timestamp']
                    # Handle timestamp with or without timezone
                    if timestamp_str.endswith('Z'):
                        timestamp_str = timestamp_str.replace('Z', '+00:00')
                    
                    dt = datetime.fromisoformat(timestamp_str)
                    # Ensure timezone is set if not already
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                        
                    # Convert to Backtrader's numeric format
                    self.lines.datetime[0] = bt.date2num(dt)
                except ValueError as e:
                    logger.error(f"Invalid timestamp format in message: {e}")
                    return False
            else:
                # If no timestamp, use current time
                self.lines.datetime[0] = bt.date2num(datetime.now(timezone.utc))
            
            # Update price data
            try:
                if 'open' in data:
                    self.lines.open[0] = float(data['open'])
                if 'high' in data:
                    self.lines.high[0] = float(data['high'])
                if 'low' in data:
                    self.lines.low[0] = float(data['low'])
                if 'close' in data:
                    self.lines.close[0] = float(data['close'])
                if 'volume' in data:
                    self.lines.volume[0] = float(data['volume'])
            except (ValueError, TypeError) as e:
                logger.error(f"Invalid data format in message: {e}")
                return False
            return True
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return False