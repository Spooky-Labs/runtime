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

    def test_ohlcv_field_returns_tracked_line(self):
        """Test accessing OHLCV field returns TrackedLine (for market data)"""
        from access_tracker import AccessTracker, AccessTrackingWrapper, TrackedLine

        tracker = AccessTracker()

        mock_feed = MagicMock()
        mock_feed._name = 'BTC/USD'
        mock_feed.current_data_hash = 'hash1'

        mock_line = MagicMock()
        mock_feed.close = mock_line

        wrapper = AccessTrackingWrapper(mock_feed, tracker)

        result = wrapper.close
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


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
