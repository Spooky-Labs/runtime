"""
News Data Feed for Backtrader

Provides real-time news from Pub/Sub as a Backtrader data feed.
One feed per news source (e.g., ALPACA_NEWS).

LINES:
- Standard OHLCV (zeros) - required for plotting/observers
- news_id, created_at_ts, updated_at_ts, symbol_count - numeric data

TEXT LOOKBACK (via __getattr__):
- headline, summary, content, author, url, source, symbols
- Access via indexing: news.headline[0], news.headline[-1]

Access in strategy:
    news = self.getdatabyname('ALPACA_NEWS')
    if len(news) > 0:
        headline = news.headline[0]      # Current article
        prev_headline = news.headline[-1] # Previous article
        symbols = news.symbols[0]         # Returns List[str]
"""

import logging
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import backtrader as bt

from news_shared_subscriber import NewsSharedSubscriber

logger = logging.getLogger(__name__)


class _TextAccessor:
    """
    Provides Backtrader-style [0], [-1] indexing for text fields.

    The history list stores articles in chronological order (oldest first).
    Backtrader uses [0] for current, [-1] for previous, etc.
    This accessor converts between the two indexing schemes.

    Usage:
        news.headline[0]   # Current article
        news.headline[-1]  # Previous article
        news.symbols[-2]   # 2 articles ago (returns List[str])
    """
    def __init__(self, history: List[Dict], field: str):
        self._history = history  # Reference to NewsDataFeed._text_history
        self._field = field      # 'headline', 'content', 'symbols', etc.

    def __getitem__(self, idx: int) -> Any:
        # Convert Backtrader index to list index
        # [0] = current (last item), [-1] = previous (second-to-last), etc.
        abs_idx = len(self._history) - 1 + idx

        if 0 <= abs_idx < len(self._history):
            value = self._history[abs_idx].get(self._field)
            # Return empty list for symbols, empty string for other fields
            if value is None:
                return [] if self._field == 'symbols' else ''
            return value

        # Out of bounds - return empty
        return [] if self._field == 'symbols' else ''

    def __len__(self):
        """Number of articles available for lookback"""
        return len(self._history)

    def __repr__(self):
        return f"_TextAccessor({self._field}, len={len(self._history)})"


class NewsDataFeed(bt.feeds.DataBase):
    """
    Backtrader data feed for real-time news.

    Numeric Lines (support standard Backtrader indexing):
    - open, high, low, close, volume: Standard OHLCV (zeros for compatibility)
    - news_id: Article ID as integer
    - created_at_ts: Unix timestamp of creation
    - updated_at_ts: Unix timestamp of last update
    - symbol_count: Number of symbols mentioned

    Text Fields (via __getattr__ + _TextAccessor):
    - headline, summary, content, author, url, source, symbols
    - Access: news.headline[0], news.headline[-1], etc.
    """

    # Standard OHLCV lines (zeros) + news-specific numeric lines
    # OHLCV required for plotting/observers to not break
    lines = ('open', 'high', 'low', 'close', 'volume',
             'news_id', 'created_at_ts', 'updated_at_ts', 'symbol_count')

    # Text fields accessible via __getattr__
    TEXT_FIELDS = {'headline', 'summary', 'content', 'author', 'url', 'source', 'symbols'}

    params = (
        ('project_id', None),
        ('source_name', 'ALPACA'),  # News source identifier
        ('agent_id', None),
        ('buffer_size', 500),  # Buffer more news items
        ('text_history_size', 1000),  # How many articles to keep for lookback
    )

    # State constants
    DELAYED = 0
    LIVE = 1
    DISCONNECTED = 2

    def __init__(self):
        super().__init__()

        self._state = self.DELAYED
        self._running = False

        # Message buffer (thread-safe deque)
        self._data_buffer = deque(maxlen=self.p.buffer_size)

        # Reference to shared subscriber
        self._shared_subscriber = None

        # Text content history for lookback support
        # Each entry is a dict with all text fields + data_hash for that article
        self._text_history: List[Dict[str, Any]] = []

        # FMEL tracking (current article's hash)
        self.current_data_hash: Optional[str] = None

        # Stats
        self._messages_processed = 0

        logger.info(f"NewsDataFeed initialized for source: {self.p.source_name}")

    # =========================================================================
    # TEXT FIELD ACCESS VIA __getattr__
    # =========================================================================

    def __getattr__(self, name: str):
        """
        Intercept access to text fields and return accessor with [idx] support.
        This enables: news.headline[0], news.headline[-1], etc.

        Note: This is only called for attributes not found through normal lookup.
        We only handle TEXT_FIELDS here; other missing attributes raise the
        standard AttributeError to allow Backtrader's metaclass to work properly.
        """
        # Only intercept TEXT_FIELDS
        if name in self.TEXT_FIELDS:
            # Need to access _text_history without triggering __getattr__ recursion
            try:
                history = object.__getattribute__(self, '_text_history')
                return _TextAccessor(history, name)
            except AttributeError:
                # _text_history not yet initialized (during Backtrader setup)
                return _TextAccessor([], name)

        # For all other attributes, raise standard AttributeError
        raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")

    # =========================================================================
    # BACKTRADER INTEGRATION
    # =========================================================================

    def islive(self) -> bool:
        """Indicate this is a live data feed"""
        return True

    def haslivedata(self) -> bool:
        """Check if live data is available"""
        return len(self._data_buffer) > 0

    def start(self) -> None:
        """Start the data feed by registering with NewsSharedSubscriber"""
        if self._running:
            return

        try:
            self._shared_subscriber = NewsSharedSubscriber.get_instance(
                self.p.project_id,
                self.p.agent_id
            )
            self._shared_subscriber.register_feed(self)

            self._running = True
            logger.info(f"NewsDataFeed started for {self.p.source_name}")

        except Exception as e:
            logger.error(f"Failed to start NewsDataFeed: {e}")
            raise

    def _load(self) -> Optional[bool]:
        """Load next news article from buffer"""
        if not self._running:
            return False

        if not self._data_buffer:
            return None  # Waiting for data

        try:
            data = self._data_buffer.popleft()
            self._messages_processed += 1

            # Parse timestamps
            created_at_str = data.get('created_at', '')
            updated_at_str = data.get('updated_at', '')

            if created_at_str:
                if created_at_str.endswith('Z'):
                    created_at_str = created_at_str[:-1] + '+00:00'
                created_dt = datetime.fromisoformat(created_at_str)
                created_ts = created_dt.timestamp()
            else:
                created_dt = datetime.now(timezone.utc)
                created_ts = created_dt.timestamp()

            if updated_at_str:
                if updated_at_str.endswith('Z'):
                    updated_at_str = updated_at_str[:-1] + '+00:00'
                updated_ts = datetime.fromisoformat(updated_at_str).timestamp()
            else:
                updated_ts = created_ts

            # Set standard OHLCV lines to zeros (required for plotting/observers)
            self.lines.datetime[0] = bt.date2num(created_dt)
            self.lines.open[0] = 0.0
            self.lines.high[0] = 0.0
            self.lines.low[0] = 0.0
            self.lines.close[0] = 0.0
            self.lines.volume[0] = 0.0

            # Set news-specific numeric lines
            self.lines.news_id[0] = float(data.get('news_id', 0) or 0)
            self.lines.created_at_ts[0] = created_ts
            self.lines.updated_at_ts[0] = updated_ts
            self.lines.symbol_count[0] = float(len(data.get('symbols', [])))

            # Store text content + hash in history for lookback
            self._text_history.append({
                'headline': data.get('headline', ''),
                'summary': data.get('summary', ''),
                'content': data.get('content', ''),
                'author': data.get('author', ''),
                'url': data.get('url', ''),
                'source': data.get('source', ''),
                'symbols': data.get('symbols', []),
                'data_hash': data.get('data_hash'),  # For FMEL tracking of lookback
            })

            # Limit history size (remove oldest when full)
            if len(self._text_history) > self.p.text_history_size:
                self._text_history.pop(0)

            # FMEL tracking (current article)
            self.current_data_hash = data.get('data_hash')

            # Update state on first message
            if self._state == self.DELAYED:
                self._state = self.LIVE

            return True

        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Invalid news data format: {e}")
            return None
        except Exception as e:
            logger.error(f"Error loading news: {e}")
            return False

    def stop(self) -> None:
        """Gracefully stop the data feed"""
        if not self._running:
            return

        logger.info(f"Stopping NewsDataFeed for {self.p.source_name}")
        self._running = False
        self._state = self.DISCONNECTED

        logger.info(
            f"NewsDataFeed stopped - "
            f"Processed: {self._messages_processed}, "
            f"History: {len(self._text_history)}"
        )

    def __del__(self):
        """Cleanup on deletion"""
        self.stop()
