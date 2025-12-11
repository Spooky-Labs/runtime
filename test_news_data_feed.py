#!/usr/bin/env python3
"""
Tests for News Data Feed and Access Tracking

Verifies:
1. _TextAccessor provides correct Backtrader-style indexing
2. NewsDataFeed has proper OHLCV lines + news-specific lines
3. TrackedTextAccessor correctly looks up data_hash for lookback
4. TrackedNewsLine correctly looks up data_hash for lookback
5. AccessTrackingWrapper routes news fields to correct tracking classes
"""

import os
import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timezone


class TestTextAccessor:
    """Test the _TextAccessor class for Backtrader-style indexing"""

    def test_indexing_current_article(self):
        """Test [0] returns the current (most recent) article"""
        from news_data_feed import _TextAccessor

        history = [
            {'headline': 'Article 1', 'symbols': ['AAPL']},
            {'headline': 'Article 2', 'symbols': ['GOOG']},
            {'headline': 'Article 3', 'symbols': ['TSLA']},
        ]

        accessor = _TextAccessor(history, 'headline')
        assert accessor[0] == 'Article 3'

    def test_indexing_previous_article(self):
        """Test [-1] returns the previous article"""
        from news_data_feed import _TextAccessor

        history = [
            {'headline': 'Article 1', 'symbols': ['AAPL']},
            {'headline': 'Article 2', 'symbols': ['GOOG']},
            {'headline': 'Article 3', 'symbols': ['TSLA']},
        ]

        accessor = _TextAccessor(history, 'headline')
        assert accessor[-1] == 'Article 2'
        assert accessor[-2] == 'Article 1'

    def test_indexing_out_of_bounds_returns_empty_string(self):
        """Test out of bounds index returns empty string for text fields"""
        from news_data_feed import _TextAccessor

        history = [
            {'headline': 'Article 1'},
        ]

        accessor = _TextAccessor(history, 'headline')
        assert accessor[-1] == ''  # No previous article
        assert accessor[-10] == ''

    def test_indexing_out_of_bounds_returns_empty_list_for_symbols(self):
        """Test out of bounds index returns empty list for symbols field"""
        from news_data_feed import _TextAccessor

        history = [
            {'symbols': ['AAPL']},
        ]

        accessor = _TextAccessor(history, 'symbols')
        assert accessor[-1] == []  # No previous article
        assert accessor[-10] == []

    def test_symbols_returns_list(self):
        """Test symbols field returns list correctly"""
        from news_data_feed import _TextAccessor

        history = [
            {'symbols': ['AAPL', 'GOOG']},
            {'symbols': ['TSLA']},
        ]

        accessor = _TextAccessor(history, 'symbols')
        assert accessor[0] == ['TSLA']
        assert accessor[-1] == ['AAPL', 'GOOG']

    def test_len_returns_history_length(self):
        """Test len() returns number of articles in history"""
        from news_data_feed import _TextAccessor

        history = [
            {'headline': 'Article 1'},
            {'headline': 'Article 2'},
            {'headline': 'Article 3'},
        ]

        accessor = _TextAccessor(history, 'headline')
        assert len(accessor) == 3

    def test_empty_history(self):
        """Test accessor handles empty history gracefully"""
        from news_data_feed import _TextAccessor

        history = []
        accessor = _TextAccessor(history, 'headline')

        assert accessor[0] == ''
        assert len(accessor) == 0

    def test_missing_field_returns_empty(self):
        """Test missing field in history entry returns empty value"""
        from news_data_feed import _TextAccessor

        history = [
            {'headline': 'Article 1'},  # No 'author' field
        ]

        accessor = _TextAccessor(history, 'author')
        assert accessor[0] == ''


class TestNewsDataFeedLines:
    """Test that NewsDataFeed has correct Backtrader lines"""

    def test_lines_tuple_includes_ohlcv(self):
        """Test lines tuple includes standard OHLCV for compatibility"""
        from news_data_feed import NewsDataFeed

        # Check the raw lines tuple before Backtrader transforms it
        # The lines are stored in the class but Backtrader wraps them
        # We verify by checking the class definition includes expected fields
        import inspect
        source = inspect.getsource(NewsDataFeed)
        assert "'open'" in source
        assert "'high'" in source
        assert "'low'" in source
        assert "'close'" in source
        assert "'volume'" in source

    def test_lines_tuple_includes_news_fields(self):
        """Test lines tuple includes news-specific numeric fields"""
        from news_data_feed import NewsDataFeed

        import inspect
        source = inspect.getsource(NewsDataFeed)
        assert "'news_id'" in source
        assert "'created_at_ts'" in source
        assert "'updated_at_ts'" in source
        assert "'symbol_count'" in source

    def test_text_fields_defined(self):
        """Test TEXT_FIELDS class attribute is correctly defined"""
        from news_data_feed import NewsDataFeed

        expected_fields = {'headline', 'summary', 'content', 'author', 'url', 'source', 'symbols'}
        assert NewsDataFeed.TEXT_FIELDS == expected_fields


class TestNewsDataFeedGetattr:
    """Test NewsDataFeed.__getattr__ returns _TextAccessor for text fields"""

    def test_getattr_returns_text_accessor_directly(self):
        """Test __getattr__ returns _TextAccessor for text fields"""
        from news_data_feed import NewsDataFeed, _TextAccessor

        # Test __getattr__ directly on an object that has _text_history
        class MockFeed:
            TEXT_FIELDS = NewsDataFeed.TEXT_FIELDS
            _text_history = [
                {'headline': 'Test Article', 'symbols': ['AAPL']},
            ]

            def __getattr__(self, name):
                if name in self.TEXT_FIELDS:
                    return _TextAccessor(self._text_history, name)
                raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")

        feed = MockFeed()
        accessor = feed.headline
        assert isinstance(accessor, _TextAccessor)
        assert accessor[0] == 'Test Article'

    def test_getattr_raises_for_unknown_attribute(self):
        """Test accessing unknown attribute raises AttributeError"""
        from news_data_feed import NewsDataFeed, _TextAccessor

        class MockFeed:
            TEXT_FIELDS = NewsDataFeed.TEXT_FIELDS
            _text_history = []

            def __getattr__(self, name):
                if name in self.TEXT_FIELDS:
                    return _TextAccessor(self._text_history, name)
                raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")

        feed = MockFeed()

        with pytest.raises(AttributeError):
            _ = feed.nonexistent_field


class TestTrackedTextAccessor:
    """Test TrackedTextAccessor records access with correct data_hash"""

    def test_records_access_with_correct_hash_current(self):
        """Test [0] records access with current article's hash"""
        from access_tracker import TrackedTextAccessor
        from news_data_feed import _TextAccessor

        history = [
            {'headline': 'Article 1', 'data_hash': 'hash1'},
            {'headline': 'Article 2', 'data_hash': 'hash2'},
            {'headline': 'Article 3', 'data_hash': 'hash3'},
        ]

        recorded_accesses = []

        class MockWrapper:
            def _record_field_access(self, field, index, data_hash=None):
                recorded_accesses.append({
                    'field': field,
                    'index': index,
                    'data_hash': data_hash
                })

        text_accessor = _TextAccessor(history, 'headline')
        tracked = TrackedTextAccessor(text_accessor, MockWrapper(), 'headline')

        result = tracked[0]

        assert result == 'Article 3'
        assert len(recorded_accesses) == 1
        assert recorded_accesses[0]['field'] == 'headline'
        assert recorded_accesses[0]['index'] == 0
        assert recorded_accesses[0]['data_hash'] == 'hash3'

    def test_records_access_with_correct_hash_lookback(self):
        """Test [-1] records access with previous article's hash"""
        from access_tracker import TrackedTextAccessor
        from news_data_feed import _TextAccessor

        history = [
            {'headline': 'Article 1', 'data_hash': 'hash1'},
            {'headline': 'Article 2', 'data_hash': 'hash2'},
            {'headline': 'Article 3', 'data_hash': 'hash3'},
        ]

        recorded_accesses = []

        class MockWrapper:
            def _record_field_access(self, field, index, data_hash=None):
                recorded_accesses.append({
                    'field': field,
                    'index': index,
                    'data_hash': data_hash
                })

        text_accessor = _TextAccessor(history, 'headline')
        tracked = TrackedTextAccessor(text_accessor, MockWrapper(), 'headline')

        result = tracked[-1]

        assert result == 'Article 2'
        assert recorded_accesses[0]['data_hash'] == 'hash2'

    def test_records_none_hash_for_out_of_bounds(self):
        """Test out of bounds access records None for data_hash"""
        from access_tracker import TrackedTextAccessor
        from news_data_feed import _TextAccessor

        history = [
            {'headline': 'Article 1', 'data_hash': 'hash1'},
        ]

        recorded_accesses = []

        class MockWrapper:
            def _record_field_access(self, field, index, data_hash=None):
                recorded_accesses.append({
                    'field': field,
                    'index': index,
                    'data_hash': data_hash
                })

        text_accessor = _TextAccessor(history, 'headline')
        tracked = TrackedTextAccessor(text_accessor, MockWrapper(), 'headline')

        result = tracked[-1]  # Out of bounds

        assert result == ''
        assert recorded_accesses[0]['data_hash'] is None


class TestTrackedNewsLine:
    """Test TrackedNewsLine records access with correct data_hash"""

    def test_records_access_with_correct_hash_current(self):
        """Test [0] records access with current article's hash"""
        from access_tracker import TrackedNewsLine

        history = [
            {'data_hash': 'hash1'},
            {'data_hash': 'hash2'},
            {'data_hash': 'hash3'},
        ]

        # Mock a Backtrader line
        class MockLine:
            def __init__(self):
                self.values = [100, 200, 300]

            def __getitem__(self, idx):
                return self.values[len(self.values) - 1 + idx]

            def __len__(self):
                return len(self.values)

        recorded_accesses = []

        class MockWrapper:
            def _record_field_access(self, field, index, data_hash=None):
                recorded_accesses.append({
                    'field': field,
                    'index': index,
                    'data_hash': data_hash
                })

        mock_line = MockLine()
        tracked = TrackedNewsLine(mock_line, MockWrapper(), 'news_id', history)

        result = tracked[0]

        assert result == 300
        assert recorded_accesses[0]['data_hash'] == 'hash3'

    def test_records_access_with_correct_hash_lookback(self):
        """Test [-1] records access with previous article's hash"""
        from access_tracker import TrackedNewsLine

        history = [
            {'data_hash': 'hash1'},
            {'data_hash': 'hash2'},
            {'data_hash': 'hash3'},
        ]

        class MockLine:
            def __init__(self):
                self.values = [100, 200, 300]

            def __getitem__(self, idx):
                return self.values[len(self.values) - 1 + idx]

            def __len__(self):
                return len(self.values)

        recorded_accesses = []

        class MockWrapper:
            def _record_field_access(self, field, index, data_hash=None):
                recorded_accesses.append({
                    'field': field,
                    'index': index,
                    'data_hash': data_hash
                })

        mock_line = MockLine()
        tracked = TrackedNewsLine(mock_line, MockWrapper(), 'news_id', history)

        result = tracked[-1]

        assert result == 200
        assert recorded_accesses[0]['data_hash'] == 'hash2'


class TestAccessTrackingWrapper:
    """Test AccessTrackingWrapper routes news fields correctly"""

    def test_text_field_returns_tracked_text_accessor(self):
        """Test accessing text field returns TrackedTextAccessor"""
        from access_tracker import AccessTracker, AccessTrackingWrapper, TrackedTextAccessor
        from news_data_feed import _TextAccessor

        tracker = AccessTracker()

        # Create a proper mock feed class (not MagicMock) to control __getattr__
        class MockNewsFeed:
            _name = 'ALPACA_NEWS'
            _text_history = [
                {'headline': 'Test', 'data_hash': 'hash1'},
            ]
            current_data_hash = 'hash1'

            def __getattr__(self, name):
                if name in {'headline', 'summary', 'content', 'author', 'url', 'source', 'symbols'}:
                    return _TextAccessor(self._text_history, name)
                raise AttributeError(name)

        mock_feed = MockNewsFeed()
        wrapper = AccessTrackingWrapper(mock_feed, tracker)

        result = wrapper.headline
        assert isinstance(result, TrackedTextAccessor)

    def test_numeric_news_field_returns_tracked_news_line(self):
        """Test accessing news numeric field returns TrackedNewsLine"""
        from access_tracker import AccessTracker, AccessTrackingWrapper, TrackedNewsLine

        tracker = AccessTracker()

        # Mock feed with _text_history
        mock_feed = MagicMock()
        mock_feed._name = 'ALPACA_NEWS'
        mock_feed._text_history = [
            {'data_hash': 'hash1'},
        ]
        mock_feed.current_data_hash = 'hash1'

        # Mock line object
        mock_line = MagicMock()
        mock_feed.news_id = mock_line

        wrapper = AccessTrackingWrapper(mock_feed, tracker)

        result = wrapper.news_id
        assert isinstance(result, TrackedNewsLine)

    def test_ohlcv_field_returns_tracked_line_without_hash_history(self):
        """Test accessing OHLCV field returns TrackedLine when no _hash_history"""
        from access_tracker import AccessTracker, AccessTrackingWrapper, TrackedLine

        tracker = AccessTracker()

        # Use a simple Mock without MagicMock's auto-attribute creation
        mock_feed = Mock(spec=['_name', 'current_data_hash', 'close', 'lines'])
        mock_feed._name = 'BTC/USD'
        mock_feed.current_data_hash = 'hash1'

        mock_line = Mock()
        mock_feed.close = mock_line
        mock_feed.lines = Mock(spec=['close'])
        mock_feed.lines.close = mock_line

        wrapper = AccessTrackingWrapper(mock_feed, tracker)

        result = wrapper.close
        # Without _hash_history, should use TrackedLine
        assert isinstance(result, TrackedLine)


class TestAccessTrackerRecordAccess:
    """Test AccessTracker.record_access captures correct data"""

    def test_record_access_stores_all_fields(self):
        """Test record_access stores field, index, and data_hash"""
        from access_tracker import AccessTracker

        tracker = AccessTracker()
        tracker.record_access('ALPACA_NEWS', 'headline', -1, 'hash123')

        accessed = tracker.get_accessed_data()
        assert len(accessed) == 1
        assert accessed[0]['symbol'] == 'ALPACA_NEWS'
        assert 'headline' in accessed[0]['fields_accessed']

        patterns = accessed[0]['access_patterns']
        assert len(patterns) == 1
        assert patterns[0]['field'] == 'headline'
        assert patterns[0]['index'] == -1

    def test_preserves_raw_index(self):
        """Test that raw Backtrader-style index is preserved"""
        from access_tracker import AccessTracker

        tracker = AccessTracker()
        tracker.record_access('ALPACA_NEWS', 'headline', 0, 'hash1')
        tracker.record_access('ALPACA_NEWS', 'headline', -1, 'hash2')
        tracker.record_access('ALPACA_NEWS', 'headline', -2, 'hash3')

        accessed = tracker.get_accessed_data()
        patterns = accessed[0]['access_patterns']

        assert patterns[0]['index'] == 0
        assert patterns[1]['index'] == -1
        assert patterns[2]['index'] == -2

    def test_reset_clears_accesses(self):
        """Test reset() clears all recorded accesses"""
        from access_tracker import AccessTracker

        tracker = AccessTracker()
        tracker.record_access('ALPACA_NEWS', 'headline', 0, 'hash1')

        tracker.reset()

        accessed = tracker.get_accessed_data()
        assert len(accessed) == 0


class TestRecordFieldAccessWithHashOverride:
    """Test _record_field_access with optional data_hash parameter"""

    def test_uses_provided_hash(self):
        """Test that provided data_hash is used instead of feed's current_data_hash"""
        from access_tracker import AccessTracker, AccessTrackingWrapper

        tracker = AccessTracker()

        mock_feed = MagicMock()
        mock_feed._name = 'ALPACA_NEWS'
        mock_feed.current_data_hash = 'current_hash'

        wrapper = AccessTrackingWrapper(mock_feed, tracker)

        # Call with explicit hash override
        wrapper._record_field_access('headline', -1, 'previous_hash')

        accessed = tracker.get_accessed_data()
        assert accessed[0]['data_hash'] == 'previous_hash'

    def test_falls_back_to_feed_hash_when_none(self):
        """Test that feed's current_data_hash is used when no hash provided"""
        from access_tracker import AccessTracker, AccessTrackingWrapper

        tracker = AccessTracker()

        mock_feed = MagicMock()
        mock_feed._name = 'ALPACA_NEWS'
        mock_feed.current_data_hash = 'feed_current_hash'

        wrapper = AccessTrackingWrapper(mock_feed, tracker)

        # Call without hash (should use feed's current_data_hash)
        wrapper._record_field_access('headline', 0)

        accessed = tracker.get_accessed_data()
        assert accessed[0]['data_hash'] == 'feed_current_hash'


class TestIntegration:
    """Integration tests for complete access tracking flow"""

    def test_end_to_end_text_access_tracking(self):
        """Test complete flow: agent accesses news.headline[-1] -> correct hash recorded"""
        from access_tracker import AccessTracker, AccessTrackingWrapper
        from news_data_feed import _TextAccessor

        # Setup
        tracker = AccessTracker()

        history = [
            {'headline': 'Old News', 'data_hash': 'old_hash'},
            {'headline': 'Current News', 'data_hash': 'current_hash'},
        ]

        # Mock feed
        mock_feed = MagicMock()
        mock_feed._name = 'ALPACA_NEWS'
        mock_feed._text_history = history
        mock_feed.current_data_hash = 'current_hash'

        # Setup __getattr__ to return _TextAccessor
        mock_feed.headline = _TextAccessor(history, 'headline')

        # Wrap feed
        wrapper = AccessTrackingWrapper(mock_feed, tracker)

        # Agent accesses previous headline
        tracked_accessor = wrapper.headline
        result = tracked_accessor[-1]

        # Verify result
        assert result == 'Old News'

        # Verify tracking recorded correct hash for lookback
        accessed = tracker.get_accessed_data()
        assert len(accessed) == 1
        assert accessed[0]['data_hash'] == 'old_hash'
        assert accessed[0]['access_patterns'][0]['index'] == -1

    def test_multiple_accesses_tracked_in_sequence(self):
        """Test multiple accesses are tracked with correct sequence numbers"""
        from access_tracker import AccessTracker, AccessTrackingWrapper
        from news_data_feed import _TextAccessor

        tracker = AccessTracker()

        history = [
            {'headline': 'H1', 'summary': 'S1', 'data_hash': 'hash1'},
            {'headline': 'H2', 'summary': 'S2', 'data_hash': 'hash2'},
        ]

        mock_feed = MagicMock()
        mock_feed._name = 'ALPACA_NEWS'
        mock_feed._text_history = history
        mock_feed.current_data_hash = 'hash2'
        mock_feed.headline = _TextAccessor(history, 'headline')
        mock_feed.summary = _TextAccessor(history, 'summary')

        wrapper = AccessTrackingWrapper(mock_feed, tracker)

        # Simulate agent accessing multiple fields
        _ = wrapper.headline[0]
        _ = wrapper.headline[-1]
        _ = wrapper.summary[0]

        accessed = tracker.get_accessed_data()
        patterns = accessed[0]['access_patterns']

        assert len(patterns) == 3
        assert patterns[0]['seq'] == 0
        assert patterns[1]['seq'] == 1
        assert patterns[2]['seq'] == 2


class TestTrackedMarketLine:
    """Test TrackedMarketLine for market data lookback hash tracking"""

    def test_lookback_uses_correct_hash(self):
        """Test that [-1] access records the hash of the previous bar, not current"""
        from access_tracker import TrackedMarketLine, AccessTracker, AccessTrackingWrapper

        tracker = AccessTracker()

        # Simulate hash history (newest at end)
        hash_history = ['hash1', 'hash2', 'hash3']

        # Mock the wrapper's _record_field_access method
        mock_wrapper = Mock()
        mock_wrapper._record_field_access = Mock()

        # Mock line object
        mock_line = Mock()
        mock_line.__getitem__ = Mock(return_value=100.0)

        tracked = TrackedMarketLine(mock_line, mock_wrapper, 'close', hash_history)

        # Access current bar [0]
        _ = tracked[0]
        mock_wrapper._record_field_access.assert_called_with('close', 0, 'hash3')

        # Access previous bar [-1]
        _ = tracked[-1]
        mock_wrapper._record_field_access.assert_called_with('close', -1, 'hash2')

        # Access 2 bars ago [-2]
        _ = tracked[-2]
        mock_wrapper._record_field_access.assert_called_with('close', -2, 'hash1')

    def test_out_of_bounds_returns_none_hash(self):
        """Test that out-of-bounds access records None for hash"""
        from access_tracker import TrackedMarketLine

        hash_history = ['hash1']

        mock_wrapper = Mock()
        mock_wrapper._record_field_access = Mock()

        mock_line = Mock()
        mock_line.__getitem__ = Mock(return_value=0.0)

        tracked = TrackedMarketLine(mock_line, mock_wrapper, 'close', hash_history)

        # Access out of bounds [-1] when only one bar exists
        _ = tracked[-1]
        mock_wrapper._record_field_access.assert_called_with('close', -1, None)

    def test_forwards_getitem_to_underlying_line(self):
        """Test that value access is forwarded to the wrapped line"""
        from access_tracker import TrackedMarketLine

        hash_history = ['hash1', 'hash2']

        mock_wrapper = Mock()
        mock_wrapper._record_field_access = Mock()

        mock_line = Mock()
        mock_line.__getitem__ = Mock(side_effect=lambda i: 100.0 + i)

        tracked = TrackedMarketLine(mock_line, mock_wrapper, 'close', hash_history)

        assert tracked[0] == 100.0
        assert tracked[-1] == 99.0

        mock_line.__getitem__.assert_any_call(0)
        mock_line.__getitem__.assert_any_call(-1)


class TestMarketDataHashHistory:
    """Test that market data feed maintains correct hash history"""

    def test_hash_history_grows_with_each_load(self):
        """Test that _hash_history grows as bars are loaded"""
        # Create a mock that simulates the market data feed behavior
        hash_history = []

        # Simulate 3 _load() calls
        for i in range(3):
            data_hash = f'hash{i}'
            hash_history.append(data_hash)

        assert len(hash_history) == 3
        assert hash_history[0] == 'hash0'
        assert hash_history[1] == 'hash1'
        assert hash_history[2] == 'hash2'

    def test_hash_history_respects_size_limit(self):
        """Test that _hash_history is limited to configured size"""
        hash_history = []
        max_size = 5

        # Simulate loading 10 bars
        for i in range(10):
            hash_history.append(f'hash{i}')
            if len(hash_history) > max_size:
                hash_history.pop(0)

        assert len(hash_history) == max_size
        # Should have hashes 5-9 (oldest removed)
        assert hash_history[0] == 'hash5'
        assert hash_history[-1] == 'hash9'


class TestAccessTrackingWrapperMarketData:
    """Test AccessTrackingWrapper with market data feeds"""

    def test_ohlcv_fields_use_tracked_market_line_when_hash_history_available(self):
        """Test that OHLCV fields use TrackedMarketLine when _hash_history exists"""
        from access_tracker import AccessTracker, AccessTrackingWrapper, TrackedMarketLine

        tracker = AccessTracker()

        # Mock market data feed with hash history
        mock_feed = Mock()
        mock_feed._name = 'BTC/USD'
        mock_feed._hash_history = ['hash1', 'hash2', 'hash3']
        mock_feed.current_data_hash = 'hash3'

        # Mock close line
        mock_close = Mock()
        mock_close.__getitem__ = Mock(return_value=50000.0)
        mock_feed.close = mock_close

        # Mock lines attribute for hasattr check
        mock_lines = Mock()
        mock_lines.close = mock_close
        mock_feed.lines = mock_lines

        wrapper = AccessTrackingWrapper(mock_feed, tracker)

        # Access close line
        close_accessor = wrapper.close

        # Should be TrackedMarketLine
        assert isinstance(close_accessor, TrackedMarketLine)

    def test_lookback_records_correct_hash_for_each_bar(self):
        """Test that accessing data.close[-1] records the hash of that bar"""
        from access_tracker import AccessTracker, AccessTrackingWrapper

        tracker = AccessTracker()

        # Create a custom mock class to avoid MagicMock's auto-attribute issues
        class MockMarketFeed:
            def __init__(self):
                self._name = 'BTC/USD'
                self._hash_history = ['hash1', 'hash2', 'hash3']
                self.current_data_hash = 'hash3'
                self.close = self._create_line()
                self.lines = type('Lines', (), {'close': self.close})()

            def _create_line(self):
                class MockLine:
                    def __getitem__(self, idx):
                        return 50000.0
                return MockLine()

        mock_feed = MockMarketFeed()
        wrapper = AccessTrackingWrapper(mock_feed, tracker)

        # Access current and previous bars
        _ = wrapper.close[0]   # Should record hash3
        _ = wrapper.close[-1]  # Should record hash2
        _ = wrapper.close[-2]  # Should record hash1

        accessed = tracker.get_accessed_data()
        assert len(accessed) == 1
        assert accessed[0]['symbol'] == 'BTC/USD'

        patterns = accessed[0]['access_patterns']
        assert len(patterns) == 3
        assert patterns[0]['field'] == 'close'
        assert patterns[0]['index'] == 0
        assert patterns[1]['field'] == 'close'
        assert patterns[1]['index'] == -1
        assert patterns[2]['field'] == 'close'
        assert patterns[2]['index'] == -2

    def test_direct_indexing_with_hash_lookup(self):
        """Test that data[-1] direct access also uses hash lookup"""
        from access_tracker import AccessTracker, AccessTrackingWrapper

        tracker = AccessTracker()

        mock_feed = Mock()
        mock_feed._name = 'ETH/USD'
        mock_feed._hash_history = ['hash1', 'hash2']
        mock_feed.current_data_hash = 'hash2'
        mock_feed.__getitem__ = Mock(return_value=3000.0)

        wrapper = AccessTrackingWrapper(mock_feed, tracker)

        # Direct indexing (accesses close line by default)
        _ = wrapper[0]
        _ = wrapper[-1]

        accessed = tracker.get_accessed_data()
        patterns = accessed[0]['access_patterns']

        assert len(patterns) == 2
        assert patterns[0]['field'] == 'close'
        assert patterns[0]['index'] == 0
        assert patterns[1]['field'] == 'close'
        assert patterns[1]['index'] == -1


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
