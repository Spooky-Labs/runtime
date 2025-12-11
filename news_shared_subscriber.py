"""
Shared Pub/Sub Subscriber for News Data

Manages a single Pub/Sub subscription for news-data topic.
Fetches full content from FMEL and routes to the news feed.
"""

import json
import logging
import threading
from typing import Dict, Optional

from google.cloud import pubsub_v1

from news_fmel_fetcher import NewsFMELFetcher

logger = logging.getLogger(__name__)


class NewsSharedSubscriber:
    """
    Shared Pub/Sub subscriber for news-data topic.

    Key features:
    1. Single subscription per agent for all news
    2. Fetches full content from FMEL before routing
    3. Caches content to avoid duplicate fetches
    """

    _instances: Dict[tuple, 'NewsSharedSubscriber'] = {}
    _instances_lock = threading.Lock()

    @classmethod
    def get_instance(cls, project_id: str, agent_id: str) -> 'NewsSharedSubscriber':
        """Get or create singleton instance"""
        key = (project_id, 'news-data', agent_id)
        with cls._instances_lock:
            if key not in cls._instances:
                cls._instances[key] = cls(project_id, agent_id)
            return cls._instances[key]

    @classmethod
    def cleanup_all(cls):
        """Stop all subscribers and clear instances"""
        with cls._instances_lock:
            for subscriber in cls._instances.values():
                subscriber.stop()
            cls._instances.clear()

    def __init__(self, project_id: str, agent_id: str):
        self.project_id = project_id
        self.agent_id = agent_id

        # Single feed reference (set by register_feed)
        self.feed = None
        self.buffer = None

        # FMEL fetcher + content cache (uses FMEL_BUCKET env var)
        self.fmel_fetcher = NewsFMELFetcher()
        self._content_cache: Dict[str, Dict] = {}
        self._cache_lock = threading.Lock()

        # Pub/Sub components
        self._subscriber_client: Optional[pubsub_v1.SubscriberClient] = None
        self._streaming_pull_future = None
        self._subscription_path: Optional[str] = None
        self._running = False
        self._started = False

        # Stats
        self._messages_received = 0
        self._messages_routed = 0

        logger.info(f"NewsSharedSubscriber created - Agent: {agent_id}")

    def register_feed(self, feed: object) -> None:
        """Register the news feed to receive all news"""
        self.feed = feed
        self.buffer = feed._data_buffer

        if not self._started:
            self._start_subscription()

    def _start_subscription(self) -> None:
        """Create and start the Pub/Sub subscription"""
        if self._running:
            return

        try:
            self._subscriber_client = pubsub_v1.SubscriberClient()

            subscription_id = f"agent-{self.agent_id}-news-data"
            self._subscription_path = f"projects/{self.project_id}/subscriptions/{subscription_id}"
            topic_path = f"projects/{self.project_id}/topics/news-data"

            # Create subscription (idempotent)
            self._create_subscription(topic_path)

            # Configure flow control
            flow_control = pubsub_v1.types.FlowControl(
                max_messages=100,
                max_bytes=50 * 1024 * 1024  # 50MB
            )

            # Start streaming pull
            self._streaming_pull_future = self._subscriber_client.subscribe(
                self._subscription_path,
                callback=self._handle_message,
                flow_control=flow_control
            )

            self._running = True
            self._started = True

            logger.info(f"NewsSharedSubscriber started - Subscription: {subscription_id}")

        except Exception as e:
            logger.error(f"Failed to start NewsSharedSubscriber: {e}")
            raise

    def _create_subscription(self, topic_path: str) -> None:
        """Create subscription with idempotent handling"""
        try:
            self._subscriber_client.get_subscription(
                request={"subscription": self._subscription_path}
            )
            logger.info(f"Reusing existing subscription: {self._subscription_path}")
        except Exception:
            try:
                self._subscriber_client.create_subscription(
                    request={
                        "name": self._subscription_path,
                        "topic": topic_path,
                        "ack_deadline_seconds": 60,  # Longer for FMEL fetch
                        "enable_message_ordering": False,
                    }
                )
                logger.info(f"Created subscription: {self._subscription_path}")
            except Exception as create_error:
                if "already exists" in str(create_error).lower():
                    logger.info(f"Subscription already exists: {self._subscription_path}")
                else:
                    raise

    def _handle_message(self, message: pubsub_v1.subscriber.message.Message) -> None:
        """Handle incoming news message"""
        self._messages_received += 1

        try:
            data = json.loads(message.data.decode('utf-8'))
            data_hash = data.get('data_hash')

            # Fetch full content from FMEL (with caching)
            full_content = self._get_or_fetch_content(data_hash)

            # Merge Pub/Sub metadata with full content
            enriched_data = {**data, **full_content}

            # Route to feed buffer
            # Note: Text content is stored in _text_history during _load()
            if self.buffer is not None:
                self.buffer.append(enriched_data)
                self._messages_routed += 1

            message.ack()

            # Periodic logging
            if self._messages_received % 100 == 0:
                logger.info(
                    f"NewsSharedSubscriber stats - "
                    f"Received: {self._messages_received}, Routed: {self._messages_routed}"
                )

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in news message: {e}")
            message.nack()
        except Exception as e:
            logger.error(f"Error handling news message: {e}")
            message.nack()

    def _get_or_fetch_content(self, data_hash: str) -> Dict:
        """Fetch from FMEL with caching"""
        if not data_hash:
            return {}

        with self._cache_lock:
            if data_hash in self._content_cache:
                return self._content_cache[data_hash]

        full_content = self.fmel_fetcher.fetch(data_hash)

        with self._cache_lock:
            self._content_cache[data_hash] = full_content
            # Limit cache to 10K entries
            if len(self._content_cache) > 10000:
                oldest = list(self._content_cache.keys())[:1000]
                for key in oldest:
                    del self._content_cache[key]

        return full_content

    def stop(self) -> None:
        """Stop the subscriber and clean up"""
        if not self._running:
            return

        self._running = False

        if self._streaming_pull_future:
            try:
                self._streaming_pull_future.cancel()
                self._streaming_pull_future.result(timeout=5)
            except Exception:
                pass

        # Delete subscription
        if self._subscriber_client and self._subscription_path:
            try:
                self._subscriber_client.delete_subscription(
                    request={"subscription": self._subscription_path}
                )
                logger.info(f"Deleted subscription: {self._subscription_path}")
            except Exception:
                pass

        if self._subscriber_client:
            try:
                self._subscriber_client.close()
            except Exception:
                pass

        logger.info(
            f"NewsSharedSubscriber stopped - "
            f"Received: {self._messages_received}, Routed: {self._messages_routed}"
        )
