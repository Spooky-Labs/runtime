"""
Access Tracking System for FMEL (Foundation Model Explainability Layer)

This module provides transparent proxies around Backtrader data feeds to track
exactly which data fields the trading agent accesses during each decision.
This is critical for explainability - we need to know what the agent "looked at"
before making each buy/sell decision.

Why This Matters:
- Regulatory compliance requires explaining AI trading decisions
- Debugging requires knowing what data influenced each trade
- Model improvement needs understanding of feature importance
- Auditing requires complete decision traceability

How It Works:
1. Data feeds are wrapped with AccessTrackingWrapper
2. When agent accesses data.close[0], wrapper intercepts and records it
3. AccessTracker collects all accesses for the current decision point
4. FMEL Analyzer writes this to BigQuery with each decision record

Tracking Classes:
- TrackedLine: Standard OHLCV lines (market data feeds)
- TrackedTextAccessor: News text fields (headline, content, etc.) with lookback support
- TrackedNewsLine: News numeric lines (news_id, created_at_ts, etc.) with lookback support

Lookback Access:
For news data, agents can look back in time: news.headline[-1] returns the previous
article's headline. The tracking system records the correct data_hash for each
lookback position, enabling full traceability.

Example - Market Data:
    # In agent's next() method:
    price = self.data.close[0]  # Tracked: field='close', index=0
    prev = self.data.close[-1]  # Tracked: field='close', index=-1

Example - News Data:
    news = self.getdatabyname('ALPACA_NEWS')
    headline = news.headline[0]       # Tracked: field='headline', index=0, data_hash=current
    prev_headline = news.headline[-1] # Tracked: field='headline', index=-1, data_hash=previous

    # FMEL records the correct data_hash for each lookback position,
    # enabling full traceability even for historical access.
"""

from typing import Dict, List, Any, Optional
from collections import defaultdict
import time


class AccessTracker:
    """
    Central coordinator for tracking field-level data access.
    Maintains a record of which data feeds and fields were accessed during each decision point.
    """

    def __init__(self):
        self.reset()

    def reset(self):
        """Reset tracking for new decision point"""
        self.accessed_feeds = defaultdict(lambda: {
            'fields': set(),
            'access_patterns': [],
            'data_hash': None
        })
        self.access_sequence = 0

    def record_access(self, feed_name: str, field: str, index: int, data_hash: Optional[str]):
        """
        Record that a specific field was accessed.

        Args:
            feed_name: Name of the data feed (e.g., 'BTC/USD')
            field: Field accessed (e.g., 'close', 'high', 'news_headline')
            index: Array index accessed (0 for current, -1 for previous)
            data_hash: Hash of the complete data point from ingestion
        """
        feed_data = self.accessed_feeds[feed_name]
        feed_data['fields'].add(field)
        feed_data['access_patterns'].append({
            'seq': self.access_sequence,
            'timestamp_ns': time.time_ns(),
            'field': field,
            'index': index
        })
        if data_hash:
            feed_data['data_hash'] = data_hash
        self.access_sequence += 1

    def get_accessed_data(self) -> List[Dict[str, Any]]:
        """
        Get structured record of all accessed data for this decision point.

        Returns:
            List of dictionaries with accessed feed information
        """
        result = []
        for feed_name, feed_data in self.accessed_feeds.items():
            if feed_data['fields']:  # Only include if fields were accessed
                result.append({
                    'symbol': feed_name,
                    'fields_accessed': list(feed_data['fields']),
                    'data_hash': feed_data['data_hash'],
                    'access_patterns': sorted(feed_data['access_patterns'], key=lambda x: x['seq'])
                })
        return result

    def get_access_count(self) -> int:
        """Get total number of field accesses"""
        return self.access_sequence


class TrackedLine:
    """
    Wrapper for Backtrader Line objects to track array access.
    Intercepts [index] operations to record access patterns.
    """

    def __init__(self, line_obj, wrapper, field_name: str):
        self._line = line_obj
        self._wrapper = wrapper
        self._field_name = field_name

    def __getitem__(self, index):
        """Track array-style access like data.close[0]"""
        # Record the access
        self._wrapper._record_field_access(self._field_name, index)
        # Return the actual value
        return self._line[index]

    def __getattr__(self, name):
        """Forward all other attributes to the wrapped line"""
        return getattr(self._line, name)

    def __len__(self):
        """Forward length calls"""
        return len(self._line)

    def __repr__(self):
        """Forward representation"""
        return repr(self._line)


class TrackedTextAccessor:
    """
    Wrapper for _TextAccessor that records access on __getitem__.
    Looks up data_hash from _text_history for correct FMEL tracking of lookback.

    When agent accesses news.headline[-1], we need to record the hash of THAT
    previous article, not the current one. This class handles that lookup.
    """

    def __init__(self, text_accessor, wrapper: 'AccessTrackingWrapper', field_name: str):
        self._accessor = text_accessor
        self._wrapper = wrapper
        self._field_name = field_name

    def __getitem__(self, index: int):
        """Track indexed access like news.headline[0] or news.headline[-1]"""
        # Look up hash for THIS article (not current)
        history = self._accessor._history
        abs_idx = len(history) - 1 + index
        data_hash = history[abs_idx].get('data_hash') if 0 <= abs_idx < len(history) else None

        # Record with correct hash for this lookback position
        self._wrapper._record_field_access(self._field_name, index, data_hash)
        return self._accessor[index]

    def __len__(self):
        """Number of articles available for lookback"""
        return len(self._accessor)

    def __repr__(self):
        return f"TrackedTextAccessor({self._field_name})"


class TrackedNewsLine:
    """
    Wrapper for news numeric lines (news_id, created_at_ts, etc.).
    Looks up data_hash from _text_history since indices correspond 1:1.

    Each _load() call advances both Backtrader lines AND appends to _text_history,
    so we can use _text_history to look up the correct hash for any index.
    """

    def __init__(self, line_obj, wrapper: 'AccessTrackingWrapper', field_name: str, history: list):
        self._line = line_obj
        self._wrapper = wrapper
        self._field_name = field_name
        self._history = history  # Reference to NewsDataFeed._text_history

    def __getitem__(self, index: int):
        """Track array-style access like news.news_id[-1]"""
        # Look up hash from text history (indices correspond 1:1)
        abs_idx = len(self._history) - 1 + index
        data_hash = self._history[abs_idx].get('data_hash') if 0 <= abs_idx < len(self._history) else None

        self._wrapper._record_field_access(self._field_name, index, data_hash)
        return self._line[index]

    def __getattr__(self, name):
        """Forward all other attributes to the wrapped line"""
        return getattr(self._line, name)

    def __len__(self):
        """Forward length calls"""
        return len(self._line)

    def __repr__(self):
        return f"TrackedNewsLine({self._field_name})"


class AccessTrackingWrapper:
    """
    Transparent proxy wrapper for data feeds that tracks field access.
    Wraps a Backtrader data feed and records which fields are accessed.

    This uses Python's dynamic attribute resolution instead of inheritance
    to avoid issues with Backtrader's metaclass system.
    """

    # Standard OHLCV fields (for market data feeds)
    TRACKED_FIELDS = {
        'open', 'high', 'low', 'close', 'volume',
        'openinterest', 'datetime',
    }

    # News text fields - accessed via _TextAccessor with [index] support
    # These return TrackedTextAccessor for FMEL tracking with correct data_hash
    NEWS_TEXT_FIELDS = {
        'headline',     # string - Headline or title
        'summary',      # string - Summary text (may be first sentence)
        'author',       # string - Original author
        'content',      # string - Full content (may contain HTML)
        'url',          # string - Article URL
        'symbols',      # List[str] - Related/mentioned symbols
        'source',       # string - News source (e.g., Benzinga)
    }

    # News numeric lines - use TrackedNewsLine for FMEL tracking with correct data_hash
    NEWS_NUMERIC_LINES = {
        'news_id',          # int - News article ID
        'created_at_ts',    # float - Unix timestamp of creation
        'updated_at_ts',    # float - Unix timestamp of last update
        'symbol_count',     # int - Number of symbols mentioned
    }

    # Line-based attributes that should be tracked
    LINE_ATTRIBUTES = {'lines', 'line', 'line0', 'line1', 'line2'}

    def __init__(self, feed, tracker: AccessTracker):
        """
        Initialize wrapper.

        Args:
            feed: The Backtrader data feed to wrap
            tracker: The AccessTracker instance for recording access
        """
        # Store wrapped feed and tracker as object attributes directly
        # to avoid __setattr__ recursion
        object.__setattr__(self, '_feed', feed)
        object.__setattr__(self, '_tracker', tracker)
        object.__setattr__(self, '_wrapped_lines', {})  # Cache for wrapped line objects

    def _record_field_access(self, field: str, index: int, data_hash: Optional[str] = None):
        """
        Record that a field was accessed.

        Args:
            field: Field name accessed
            index: Array index accessed (0 for current, -1 for previous, etc.)
            data_hash: Optional hash override. If None, uses feed's current_data_hash.
                       For lookback access (index < 0), callers should provide the
                       correct hash for that historical position.
        """
        # Use provided hash, or fall back to feed's current_data_hash
        if data_hash is None:
            data_hash = getattr(self._feed, 'current_data_hash', None)

        # Record the access
        self._tracker.record_access(
            feed_name=self._name,
            field=field,
            index=index,
            data_hash=data_hash
        )

    def __getattr__(self, name):
        """
        Intercept attribute access to track field usage.
        """
        # Get the actual attribute from the wrapped feed
        attr = getattr(self._feed, name)

        # News text fields - wrap _TextAccessor with TrackedTextAccessor
        # This enables proper hash lookup for lookback access like news.headline[-1]
        if name in self.NEWS_TEXT_FIELDS:
            return TrackedTextAccessor(attr, self, name)

        # News numeric lines - use TrackedNewsLine with hash lookup from _text_history
        # This enables proper hash lookup for lookback access like news.news_id[-1]
        if name in self.NEWS_NUMERIC_LINES:
            history = getattr(self._feed, '_text_history', [])
            return TrackedNewsLine(attr, self, name, history)

        # Standard OHLCV tracked fields (for market data feeds)
        if name in self.TRACKED_FIELDS:
            # Wrap line objects to track array access
            if name not in self._wrapped_lines:
                self._wrapped_lines[name] = TrackedLine(attr, self, name)
            return self._wrapped_lines[name]

        # Check for dynamic fields (added by data_feed.py)
        # These would be any line attributes not in standard fields
        if hasattr(attr, '__getitem__') and hasattr(self._feed.lines, name):
            # This is a dynamic field line
            if name not in self._wrapped_lines:
                self._wrapped_lines[name] = TrackedLine(attr, self, name)
            return self._wrapped_lines[name]

        # Handle lines.X access pattern
        if name == 'lines':
            return TrackedLines(attr, self)

        # For all other attributes, pass through
        return attr

    def __getitem__(self, index):
        """
        Track direct array access like data[0].
        This accesses the close line by default in Backtrader.
        """
        self._record_field_access('close', index)
        return self._feed[index]

    def __setattr__(self, name, value):
        """Handle attribute setting"""
        if name in ('_feed', '_tracker', '_wrapped_lines'):
            # Internal attributes on wrapper itself
            object.__setattr__(self, name, value)
        else:
            # Pass through to wrapped feed
            setattr(self._feed, name, value)

    def __repr__(self):
        """Representation"""
        return f"AccessTrackingWrapper({self._feed._name})"

    def __len__(self):
        """Forward length calls to wrapped feed"""
        return len(self._feed)

    def __bool__(self):
        """Forward boolean evaluation to wrapped feed"""
        return bool(self._feed)


class TrackedLines:
    """
    Wrapper for the 'lines' attribute to track access to lines.X patterns.
    """

    def __init__(self, lines_obj, wrapper: AccessTrackingWrapper):
        self._lines = lines_obj
        self._wrapper = wrapper
        self._cached = {}

    def __getattr__(self, name):
        """Track access to lines.field"""
        attr = getattr(self._lines, name)

        # Check if this is a line that should be tracked
        if hasattr(attr, '__getitem__'):
            if name not in self._cached:
                self._cached[name] = TrackedLine(attr, self._wrapper, name)
            return self._cached[name]

        return attr

    def __getitem__(self, index):
        """Handle lines[index] access"""
        return self._lines[index]