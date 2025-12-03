"""
Shared Pub/Sub Subscriber for Market Data

This module manages a single Pub/Sub subscription per topic, shared across all
data feeds for that topic. This dramatically reduces subscription count from
O(symbols × agents) to O(topics × agents).

Architecture:
    ┌──────────────────────────────────────┐
    │  SharedSubscriber (1 subscription)   │
    │  Routes messages to correct feed     │
    └──────────────────────────────────────┘
            ↓           ↓           ↓
    ┌───────────┐ ┌───────────┐ ┌───────────┐
    │ Feed AAPL │ │ Feed MSFT │ │ Feed GOOG │
    └───────────┘ └───────────┘ └───────────┘

Why This Pattern:
- Pub/Sub has a hard limit of 10,000 subscriptions per topic/project
- Each agent trading 70 symbols would need 70 subscriptions
- With 100+ agents, we exceed the quota
- This pattern: 2 subscriptions per agent (equities + crypto) → supports 5,000 agents

Usage:
    # In data_feed.py
    subscriber = SharedSubscriber.get_instance(project_id, topic_name, agent_id)
    subscriber.register_feed(symbol, feed)
"""

import json
import logging
import threading
from typing import Dict, Optional

from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


class SharedSubscriber:
    """
    Singleton per topic that manages a shared Pub/Sub subscription.

    One instance is created per (project_id, topic_name, agent_id) combination.
    All feeds for the same topic register with the shared subscriber, which
    routes incoming messages to the correct feed based on symbol.

    Thread Safety:
    - Uses threading.Lock for feed registration
    - Pub/Sub callback runs on a separate thread pool
    - Feed buffers are thread-safe (collections.deque)
    """

    # Class-level storage for singleton instances
    # Key: (project_id, topic_name, agent_id) → SharedSubscriber instance
    _instances: Dict[tuple, 'SharedSubscriber'] = {}
    _instances_lock = threading.Lock()

    @classmethod
    def get_instance(cls, project_id: str, topic_name: str, agent_id: str) -> 'SharedSubscriber':
        """
        Get or create a SharedSubscriber for the given topic.

        Args:
            project_id: GCP project ID
            topic_name: Pub/Sub topic name (e.g., 'market-data', 'crypto-data')
            agent_id: Unique agent identifier (Firebase agent ID)

        Returns:
            SharedSubscriber instance for this topic
        """
        key = (project_id, topic_name, agent_id)

        with cls._instances_lock:
            if key not in cls._instances:
                cls._instances[key] = cls(project_id, topic_name, agent_id)
            return cls._instances[key]

    @classmethod
    def cleanup_all(cls):
        """Stop all shared subscribers and clear instances."""
        with cls._instances_lock:
            for subscriber in cls._instances.values():
                subscriber.stop()
            cls._instances.clear()

    def __init__(self, project_id: str, topic_name: str, agent_id: str):
        """
        Initialize a shared subscriber for a topic.

        Note: Use get_instance() instead of calling this directly.
        """
        self.project_id = project_id
        self.topic_name = topic_name
        self.agent_id = agent_id

        # Registry of feeds and their buffers by symbol
        # symbol → PubSubMarketDataFeed instance (for reference only)
        self.feeds: Dict[str, object] = {}
        # symbol → deque (direct buffer reference for thread-safe callback access)
        # IMPORTANT: We cache buffer references because accessing ANY attribute on
        # a Backtrader feed object (even _data_buffer) triggers Backtrader's
        # __getattribute__ machinery which accesses internal arrays. This causes
        # "array index out of range" errors when done from Pub/Sub's callback thread.
        self.buffers: Dict[str, object] = {}
        self._feeds_lock = threading.Lock()

        # Pub/Sub components
        self._subscriber_client: Optional[pubsub_v1.SubscriberClient] = None
        self._streaming_pull_future = None
        self._subscription_path: Optional[str] = None

        # State
        self._running = False
        self._started = False

        # Stats
        self._messages_received = 0
        self._messages_routed = 0
        self._messages_dropped = 0

        logger.info(
            f"SharedSubscriber created - "
            f"Topic: {topic_name}, Agent: {agent_id}"
        )

    def register_feed(self, symbol: str, feed: object) -> None:
        """
        Register a data feed to receive messages for a symbol.

        Args:
            symbol: The symbol this feed handles (e.g., 'AAPL', 'BTC/USD')
            feed: PubSubMarketDataFeed instance with _data_buffer attribute

        Note:
            We cache feed._data_buffer at registration time (on main thread)
            because accessing it from the Pub/Sub callback thread would trigger
            Backtrader's __getattribute__ and cause "array index out of range"
            errors. The cached deque reference is safe to use from any thread.
        """
        with self._feeds_lock:
            self.feeds[symbol] = feed
            # Cache the buffer reference NOW (on main thread) so the callback
            # thread never accesses any Backtrader object attributes
            self.buffers[symbol] = feed._data_buffer
            logger.debug(f"Registered feed for {symbol} on {self.topic_name}")

            # Start subscription if this is the first feed
            if not self._started and len(self.feeds) == 1:
                self._start_subscription()

    def unregister_feed(self, symbol: str) -> None:
        """
        Unregister a feed for a symbol.

        Args:
            symbol: The symbol to unregister
        """
        with self._feeds_lock:
            if symbol in self.feeds:
                del self.feeds[symbol]
            if symbol in self.buffers:
                del self.buffers[symbol]
            logger.debug(f"Unregistered feed for {symbol} on {self.topic_name}")

            # Stop subscription if no more feeds
            if not self.feeds and self._running:
                self._stop_subscription()

    def _start_subscription(self) -> None:
        """Create and start the shared Pub/Sub subscription."""
        if self._running:
            return

        try:
            self._subscriber_client = pubsub_v1.SubscriberClient()

            # Build subscription path
            # Format: agent-{agent_id}-{topic_name}
            # This is deterministic and agent-scoped for easy cleanup
            subscription_id = f"agent-{self.agent_id}-{self.topic_name}"
            self._subscription_path = (
                f"projects/{self.project_id}/subscriptions/{subscription_id}"
            )
            topic_path = f"projects/{self.project_id}/topics/{self.topic_name}"

            # Create subscription (idempotent - reuse if exists)
            self._create_subscription(topic_path)

            # Configure flow control
            # Max outstanding messages to prevent memory issues
            flow_control = pubsub_v1.types.FlowControl(
                max_messages=1000,
                max_bytes=100 * 1024 * 1024  # 100MB
            )

            # Start streaming pull
            self._streaming_pull_future = self._subscriber_client.subscribe(
                self._subscription_path,
                callback=self._handle_message,
                flow_control=flow_control
            )

            self._running = True
            self._started = True

            logger.info(
                f"SharedSubscriber started - "
                f"Subscription: {subscription_id}, "
                f"Topic: {self.topic_name}"
            )

        except Exception as e:
            logger.error(f"Failed to start SharedSubscriber: {e}")
            raise

    def _create_subscription(self, topic_path: str) -> None:
        """Create subscription with idempotent handling."""
        try:
            # Try to get existing subscription
            self._subscriber_client.get_subscription(
                request={"subscription": self._subscription_path}
            )
            logger.info(f"Reusing existing subscription: {self._subscription_path}")

        except Exception:
            # Subscription doesn't exist, create it
            try:
                self._subscriber_client.create_subscription(
                    request={
                        "name": self._subscription_path,
                        "topic": topic_path,
                        # No filter - receive ALL messages from topic
                        "ack_deadline_seconds": 30,
                        "enable_message_ordering": False,
                    }
                )
                logger.info(f"Created new subscription: {self._subscription_path}")

            except Exception as create_error:
                # Handle race condition where another process created it
                if "already exists" in str(create_error).lower():
                    logger.info(f"Subscription already exists: {self._subscription_path}")
                else:
                    raise

    def _handle_message(self, message: pubsub_v1.subscriber.message.Message) -> None:
        """
        Handle incoming Pub/Sub message by routing to the correct feed.

        This callback runs on Pub/Sub's thread pool, not the main thread.
        Feed buffers (deque) are thread-safe for append operations.
        """
        self._messages_received += 1

        try:
            # Parse message
            data = json.loads(message.data.decode('utf-8'))
            symbol = data.get('symbol')

            if not symbol:
                logger.warning("Message missing symbol, dropping")
                message.ack()
                self._messages_dropped += 1
                return

            # Route to registered feed's buffer
            # CRITICAL: We use self.buffers (cached deque references) instead of
            # accessing feed._data_buffer because any attribute access on a
            # Backtrader object triggers its __getattribute__ machinery, which
            # accesses internal arrays that are not thread-safe.
            with self._feeds_lock:
                buffer = self.buffers.get(symbol)

            if buffer is not None:
                # Append directly to the cached deque (thread-safe operation)
                buffer.append(data)
                self._messages_routed += 1
            else:
                # No feed registered for this symbol
                self._messages_dropped += 1

            # Always acknowledge to prevent redelivery
            message.ack()

            # Periodic logging
            if self._messages_received % 1000 == 0:
                logger.info(
                    f"SharedSubscriber {self.topic_name} stats - "
                    f"Received: {self._messages_received}, "
                    f"Routed: {self._messages_routed}, "
                    f"Dropped: {self._messages_dropped}"
                )

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message: {e}")
            message.nack()
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            message.nack()

    def _stop_subscription(self) -> None:
        """Stop the streaming pull subscription."""
        if not self._running:
            return

        self._running = False

        # Cancel streaming pull
        if self._streaming_pull_future:
            try:
                self._streaming_pull_future.cancel()
                self._streaming_pull_future.result(timeout=5)
            except Exception as e:
                logger.debug(f"Subscription cancellation: {e}")

        logger.info(
            f"SharedSubscriber stopped - "
            f"Topic: {self.topic_name}, "
            f"Received: {self._messages_received}, "
            f"Routed: {self._messages_routed}"
        )

    def stop(self) -> None:
        """
        Stop the shared subscriber and delete the subscription.

        Called during graceful shutdown to clean up Pub/Sub resources.
        """
        self._stop_subscription()

        # Delete subscription to prevent orphans
        if self._subscriber_client and self._subscription_path:
            try:
                self._subscriber_client.delete_subscription(
                    request={"subscription": self._subscription_path}
                )
                logger.info(f"Deleted subscription: {self._subscription_path}")
            except Exception as e:
                logger.debug(f"Subscription deletion: {e}")

        # Close client
        if self._subscriber_client:
            try:
                self._subscriber_client.close()
            except Exception as e:
                logger.debug(f"Subscriber close: {e}")

        # Clear feeds and buffers
        with self._feeds_lock:
            self.feeds.clear()
            self.buffers.clear()
