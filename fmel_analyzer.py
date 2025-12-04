"""
Foundation Model Explainability Layer (FMEL) Analyzer

This module records every Foundation Model trading decision to BigQuery for
complete explainability. It enables understanding "why did the AI make this trade?"
by capturing exactly what data the model accessed before each decision.

It captures:
- Decision category: INACTIVE (warming up), HOLD (no trade), or TRADED (executed trades)
- Unified event timeline: All data accesses and trades in chronological order
- Data accessed: Which fields the model read (e.g., close price, volume, news sentiment)
- Access patterns: Exact sequence and timing of data accesses (nanosecond precision)
- Portfolio state: Value, cash, and positions at decision time

Why FMEL:
- Explainability: Show exactly what data influenced each AI trading decision
- Reproducibility: Replay any decision with the exact same inputs the model saw
- Regulatory: Audit trail explaining why each trade was made
- Debugging: Trace bad decisions to their data inputs
- Analysis: Understand Foundation Model behavior patterns

Architecture Decision - Analyzer vs Observer:
We use Backtrader's Analyzer pattern instead of Observer because:
1. Analyzers don't store historical lines (memory efficient)
2. Analyzers receive direct order/trade notifications
3. Analyzers can scale to thousands of data feeds
4. Analyzers have explicit stop() for cleanup

BigQuery Integration:
Decisions are batched (10 records or 5 seconds) to reduce API calls.
Each decision includes a unified event_timeline with all data accesses
and trades that map directly to BigQuery's schema (see bigquery_schema.json).
"""

import backtrader as bt
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional, Dict, List
from google.cloud import bigquery
from google.api_core import retry

from access_tracker import AccessTracker, AccessTrackingWrapper

logger = logging.getLogger(__name__)


class FMELAnalyzer(bt.Analyzer):
    """
    FMEL Analyzer for tracking trading decisions with field-level data access.

    Advantages over Observer:
    - No line storage (memory efficient)
    - Direct order/trade notifications
    - Field-level access tracking
    - Scales to thousands of data feeds
    """

    params = (
        # Unique identifier for this trading agent (e.g., "momentum_v2")
        # Used to filter decisions by agent in BigQuery queries
        ('agent_id', None),

        # Google Cloud project ID for BigQuery writes
        ('project_id', None),

        # BigQuery dataset name - always 'fmel' for Foundation Model Explainability Layer
        ('dataset_id', 'fmel'),

        # BigQuery table name - always 'decisions' (matches Infrastructure/bigquery.tf schema)
        ('table_id', 'decisions'),

        # Number of decisions to buffer before flushing to BigQuery
        # Higher values reduce API calls but increase memory usage and data loss risk
        ('batch_size', 10),

        # Maximum seconds to wait before flushing partial batch
        # Ensures timely writes even during slow trading periods
        ('batch_timeout', 5.0),

        # AccessTracker instance for recording which data fields the agent reads
        # If None, data access tracking is disabled
        ('access_tracker', None),

        # List of Backtrader data feeds to wrap with access tracking
        # Each feed will be replaced with an AccessTrackingWrapper in start()
        ('data_feeds', None),
    )

    def __init__(self):
        """Initialize the FMEL analyzer"""
        super().__init__()

        # Unique identifier for this trading session, combining agent_id with
        # nanosecond timestamp. Used to group all decisions from a single run
        # and enables querying "show me everything from this specific backtest"
        timestamp_ns = time.time_ns()
        self.session_id = f"{self.p.agent_id}_{timestamp_ns}"

        # Sequential counter of decision points within this session
        # Increments each bar (prenext, nextstart, next) for ordering decisions
        self.decision_count = 0

        # Temporary storage for the decision currently being recorded
        # Contains bar_time, stage, portfolio state at decision start
        # Set in prenext/nextstart/next(), consumed in _end_decision_point()
        self._current_decision = None

        # Collects TRADE events during a single decision point
        # Populated by notify_order() when orders complete, then merged with
        # DATA_ACCESS events in _build_event_timeline() for chronological replay
        self._event_timeline = []

        # BigQuery client instance, initialized in start() via _setup_bigquery()
        # Reused for all writes during the session
        self.bq_client = None

        # Reference to the BigQuery table (project.dataset.table)
        # Set once in _setup_bigquery() and used for all batch inserts
        self.table_ref = None

        # Buffer holding decisions waiting to be written to BigQuery
        # Flushed when size reaches batch_size or after batch_timeout seconds
        self._batch_buffer = []

        # Thread lock protecting _batch_buffer from concurrent access
        # Required because _flush_batch_async() runs on a Timer thread
        self._batch_lock = threading.Lock()

        # Timer that triggers batch flush after batch_timeout seconds of inactivity
        # Ensures partial batches are written even during slow trading periods
        # Cancelled and reset each time a new decision is added to the buffer
        self._batch_timer = None

        # Reference to the AccessTracker that records data field accesses
        # Shared with AccessTrackingWrapper instances that wrap data feeds
        self.access_tracker = self.p.access_tracker

        logger.info(f"FMEL Analyzer initialized - Session: {self.session_id}")

    def start(self):
        """Called when analyzer starts"""
        # Initialize BigQuery
        self._setup_bigquery()

        # Wrap the strategy's data feeds to track access
        # Note: We wrap here instead of in runner.py to avoid timing issues with
        # Backtrader's initialization sequence. Wrapping after strategy.__init__
        # ensures the Agent doesn't encounter wrapped objects during setup,
        # while still capturing all data access during actual trading decisions.
        if self.p.data_feeds and self.access_tracker:
            # Replace each data feed in strategy.datas with wrapped version
            for i, feed in enumerate(self.strategy.datas):
                if i < len(self.p.data_feeds):
                    # Create wrapper for this feed
                    wrapped = AccessTrackingWrapper(feed, self.access_tracker)
                    # Replace the data feed in the strategy's datas list
                    self.strategy.datas[i] = wrapped
                    logger.debug(f"Wrapped data feed: {feed._name}")

        # Log initial state
        logger.info(
            f"FMEL tracking started - "
            f"Agent: {self.p.agent_id}, "
            f"Session: {self.session_id}, "
            f"Symbols: {[d._name for d in self.strategy.datas]}"
        )

        # Log session start (not written to BigQuery - doesn't match decisions schema)
        logger.info(
            f"Session started - "
            f"Symbols: {[d._name for d in self.strategy.datas]}, "
            f"Initial cash: ${self.strategy.broker.getcash():,.2f}, "
            f"Initial value: ${self.strategy.broker.getvalue():,.2f}"
        )

    def prenext(self):
        """Called after Strategy.prenext() completes.

        Strategy may have used prenext() -> self.next() pattern for multi-feed trading.
        Don't reset tracker/timeline BEFORE recording - capture what the strategy actually did.
        """
        self.decision_count += 1
        self._current_decision = {
            'decision_point': self.decision_count,
            'stage': 'PRENEXT',
            'bar_time': bt.num2date(self.strategy.datetime[0]),
        }
        # No forced_action - _end_decision_point will determine based on actual activity
        self._end_decision_point()
        # Reset for next bar (AFTER recording)
        if self.access_tracker:
            self.access_tracker.reset()
        self._event_timeline = []

    def nextstart(self):
        """Called after Strategy.nextstart() completes."""
        self.decision_count += 1
        self._current_decision = {
            'decision_point': self.decision_count,
            'stage': 'NEXTSTART',
            'bar_time': bt.num2date(self.strategy.datetime[0]),
        }
        self._end_decision_point()
        # Reset for next bar
        if self.access_tracker:
            self.access_tracker.reset()
        self._event_timeline = []

    def next(self):
        """Called after Strategy.next() completes."""
        self.decision_count += 1
        self._current_decision = {
            'decision_point': self.decision_count,
            'stage': 'NEXT',
            'bar_time': bt.num2date(self.strategy.datetime[0]),
        }
        self._end_decision_point()
        # Reset for next bar
        if self.access_tracker:
            self.access_tracker.reset()
        self._event_timeline = []

    def notify_order(self, order):
        """
        Called when order status changes.
        Track all completed orders for the current decision point.
        """
        if order.status == order.Completed:
            action = 'BUY' if order.isbuy() else 'SELL'
            timestamp_ns = time.time_ns()
            symbol = order.data._name

            # Add to unified event timeline (for replay and financial analysis)
            self._event_timeline.append({
                'timestamp_ns': timestamp_ns,
                'event_type': 'TRADE',
                'symbol': symbol,
                'field': None,  # Not applicable for trades
                'index': None,  # Not applicable for trades
                'action': action,
                'size': float(order.executed.size),
                'price': float(order.executed.price),
                'value': float(order.executed.value) if order.executed.value else None,
                'commission': float(order.executed.comm) if order.executed.comm else None,
                'pnl': float(order.executed.pnl) if getattr(order.executed, 'pnl', None) else None,
                'data_hash': None  # Trades don't reference data_registry
            })

            logger.debug(
                f"Order completed - {action} {symbol} "
                f"Size: {order.executed.size} @ {order.executed.price}"
            )

    def _end_decision_point(self):
        """Finalize and record the decision point"""
        if not self._current_decision:
            return

        # Get accessed data with field details (needed for action category determination)
        accessed_data = []
        if self.access_tracker:
            accessed_data = self.access_tracker.get_accessed_data()

        # Determine final action category based on what the strategy ACTUALLY did:
        # TRADED: One or more trades executed
        # HOLD: Strategy accessed data but didn't trade (made conscious decision)
        # INACTIVE: No data accessed, no trades (true warmup - strategy couldn't act)
        if any(e['event_type'] == 'TRADE' for e in self._event_timeline):
            action_category = 'TRADED'
        elif accessed_data:
            action_category = 'HOLD'
        else:
            action_category = 'INACTIVE'

        # Build unified event timeline by merging data accesses and trades
        event_timeline = self._build_event_timeline(accessed_data)

        # Record the complete decision
        self._record_decision(action_category, accessed_data, event_timeline)

    def _build_event_timeline(self, accessed_data: List[Dict]) -> List[Dict]:
        """
        Build unified timeline of data accesses and trades in chronological order.
        This enables replaying the agent's decision process step-by-step.
        """
        timeline = []

        # Add data access events from access_tracker
        for feed_data in accessed_data:
            symbol = feed_data.get('symbol')
            data_hash = feed_data.get('data_hash')
            for access in feed_data.get('access_patterns', []):
                timeline.append({
                    'timestamp_ns': access['timestamp_ns'],
                    'event_type': 'DATA_ACCESS',
                    'symbol': symbol,
                    'field': access['field'],
                    'index': access['index'],
                    'action': None,  # Not applicable for data access
                    'size': None,
                    'price': None,
                    'value': None,
                    'commission': None,
                    'pnl': None,
                    'data_hash': data_hash
                })

        # Add trade events (already in _event_timeline from notify_order)
        timeline.extend(self._event_timeline)

        # Sort by timestamp to get true chronological order
        timeline.sort(key=lambda x: x['timestamp_ns'])

        # Assign global sequence numbers
        for seq, event in enumerate(timeline, start=1):
            event['seq'] = seq

        return timeline

    def _record_decision(self, action: str, accessed_data: Optional[List] = None, event_timeline: Optional[List] = None):
        """Record a decision with all context"""
        if not self._current_decision:
            return

        # Get current positions
        positions = self._get_positions()

        # Build decision record matching BigQuery schema
        decision = {
            'session_id': self.session_id,
            'agent_id': self.p.agent_id,
            'decision_point': self._current_decision['decision_point'],
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'bar_time': self._current_decision['bar_time'].isoformat(),
            'stage': self._current_decision['stage'],
            'action': action,
            'portfolio_value': float(self.strategy.broker.getvalue()),
            'portfolio_cash': float(self.strategy.broker.getcash()),
            'positions': positions,
            'data_accessed': accessed_data or [],
            'access_count': self.access_tracker.get_access_count() if self.access_tracker else 0,
            'event_timeline': event_timeline or [],  # Unified timeline for replay (includes trade details)
        }

        # Add to batch
        self._add_to_batch(decision)

        # Log decision
        logger.debug(
            f"Decision {self.decision_count}: {action} - "
            f"Value: ${decision['portfolio_value']:,.2f}, "
            f"Accessed: {len(decision['data_accessed'])} feeds, "
            f"Events: {len(decision['event_timeline'])}"
        )

    def _get_positions(self) -> List[Dict]:
        """Get current positions from broker"""
        positions = []
        for data in self.strategy.datas:
            pos = self.strategy.getposition(data)
            if pos.size != 0:
                positions.append({
                    'symbol': data._name,
                    'size': float(pos.size),
                    'price': float(pos.price),
                    'value': float(pos.size * pos.price),
                })
        return positions

    def _setup_bigquery(self):
        """Initialize BigQuery client and table reference"""
        try:
            self.bq_client = bigquery.Client(project=self.p.project_id)

            dataset_ref = self.bq_client.dataset(self.p.dataset_id)
            self.table_ref = dataset_ref.table(self.p.table_id)

            # Verify table exists
            try:
                self.bq_client.get_table(self.table_ref)
                logger.info(f"BigQuery table verified: {self.p.dataset_id}.{self.p.table_id}")
            except Exception as e:
                logger.warning(f"BigQuery table not found: {e}")
                logger.info("Table will be created on first write if schema exists")

        except Exception as e:
            logger.error(f"Failed to setup BigQuery: {e}")
            raise

    def _add_to_batch(self, decision: Dict):
        """Add decision to batch with automatic flushing"""
        with self._batch_lock:
            self._batch_buffer.append(decision)

            # Check if batch is full
            if len(self._batch_buffer) >= self.p.batch_size:
                self._flush_batch()
            else:
                # Reset timer for timeout-based flush
                if self._batch_timer:
                    self._batch_timer.cancel()
                self._batch_timer = threading.Timer(
                    self.p.batch_timeout,
                    self._flush_batch_async
                )
                self._batch_timer.daemon = True
                self._batch_timer.start()

    def _flush_batch_async(self):
        """Async wrapper for batch flushing"""
        with self._batch_lock:
            self._flush_batch()

    def _flush_batch(self):
        """Flush current batch to BigQuery"""
        if not self._batch_buffer:
            return

        # Copy and clear buffer
        decisions = list(self._batch_buffer)
        self._batch_buffer.clear()

        # Cancel timer
        if self._batch_timer:
            self._batch_timer.cancel()
            self._batch_timer = None

        # Write to BigQuery
        try:
            errors = self.bq_client.insert_rows_json(
                self.table_ref,
                decisions,
                retry=retry.Retry(deadline=30)
            )

            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
            else:
                logger.info(f"Flushed {len(decisions)} decisions to BigQuery")

        except Exception as e:
            logger.error(f"Failed to write to BigQuery: {e}")
            # Re-add to buffer for retry
            self._batch_buffer.extend(decisions)

    def stop(self):
        """Called when analyzer stops"""
        # Record final decision if pending
        if self._current_decision:
            self._end_decision_point()

        # Log session end (not written to BigQuery - doesn't match decisions schema)
        logger.info(
            f"Session ended - "
            f"Final value: ${self.strategy.broker.getvalue():,.2f}, "
            f"Final cash: ${self.strategy.broker.getcash():,.2f}, "
            f"Total decisions: {self.decision_count}"
        )

        # Flush any remaining batches
        with self._batch_lock:
            self._flush_batch()

        # Cancel timer
        if self._batch_timer:
            self._batch_timer.cancel()

        # Log summary
        logger.info(
            f"FMEL session complete - "
            f"Session: {self.session_id}, "
            f"Decisions: {self.decision_count}, "
            f"Final Value: ${self.strategy.broker.getvalue():,.2f}"
        )

    def get_analysis(self):
        """Return analysis results (required by Analyzer interface)"""
        return {
            'session_id': self.session_id,
            'decision_count': self.decision_count,
            'final_value': self.strategy.broker.getvalue(),
            'final_cash': self.strategy.broker.getcash(),
        }