"""
Foundation Model Explainability Layer (FMEL) Analyzer

This module records every trading decision to BigQuery for complete explainability.
It captures:
- What action was taken (BUY/SELL/HOLD)
- What data the agent accessed before deciding
- Portfolio state before and after
- Timing information for performance analysis

Why FMEL:
- Regulatory: Explain why each trade was made
- Debugging: Trace bad decisions to their data inputs
- Analysis: Understand agent behavior patterns
- Auditing: Complete audit trail of all decisions

Architecture Decision - Analyzer vs Observer:
We use Backtrader's Analyzer pattern instead of Observer because:
1. Analyzers don't store historical lines (memory efficient)
2. Analyzers receive direct order/trade notifications
3. Analyzers can scale to thousands of data feeds
4. Analyzers have explicit stop() for cleanup

BigQuery Integration:
Decisions are batched (10 records or 5 seconds) to reduce API calls.
Each decision includes nested structures for actions and data access
that map directly to BigQuery's schema (see bigquery_schema.json).
"""

import backtrader as bt
import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional, Dict, List
from google.cloud import bigquery
from google.api_core import retry

from access_tracker import AccessTracker

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
        ('agent_id', None),
        ('project_id', None),
        ('dataset_id', 'fmel'),
        ('table_id', 'decisions'),
        ('batch_size', 10),
        ('batch_timeout', 5.0),
        ('access_tracker', None),  # AccessTracker instance
        ('data_feeds', None),  # List of data feeds to wrap
    )

    def __init__(self):
        """Initialize the FMEL analyzer"""
        super().__init__()

        # Session tracking
        timestamp = int(time.time())
        self.session_id = f"{self.p.agent_id}_{timestamp}"

        # Decision tracking
        self.decision_count = 0
        self._current_decision = None
        self._decision_actions = []  # Actions for current decision point
        self._action_sequence = 0

        # BigQuery setup
        self.bq_client = None
        self.table_ref = None
        self._batch_buffer = deque()
        self._batch_lock = threading.Lock()
        self._batch_timer = None

        # Access tracking
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
            from access_tracker import AccessTrackingWrapper

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

    def prenext(self):
        """Called before minimum period for all datas/indicators"""
        self._start_decision_point('PRENEXT')
        self._record_decision('INACTIVE')
        self._end_decision_point()

    def nextstart(self):
        """Called exactly once when minimum period is reached"""
        self._start_decision_point('NEXTSTART')
        self._record_decision('HOLD')
        self._end_decision_point()

    def next(self):
        """Called for each bar after minimum period"""
        # End previous decision point if exists
        if self._current_decision and self._current_decision['stage'] == 'NEXT':
            self._end_decision_point()

        # Start new decision point
        self._start_decision_point('NEXT')
        # Strategy has already run at this point (Analyzer.next() is called after Strategy.next())

    def notify_order(self, order):
        """
        Called when order status changes.
        Track all completed orders for the current decision point.
        """
        if order.status == order.Completed:
            action = 'BUY' if order.isbuy() else 'SELL'

            # Add to current decision's actions
            self._decision_actions.append({
                'seq': self._action_sequence,
                'timestamp_ns': time.time_ns(),
                'action': action,
                'symbol': order.data._name,
                'size': order.executed.size,
                'price': order.executed.price,
                'value': order.executed.value,
                'commission': order.executed.comm,
                'pnl': getattr(order.executed, 'pnl', None)
            })
            self._action_sequence += 1

            logger.debug(
                f"Order completed - {action} {order.data._name} "
                f"Size: {order.executed.size} @ {order.executed.price}"
            )

    def notify_trade(self, trade):
        """Called when a trade is opened/updated/closed"""
        if trade.isclosed:
            logger.debug(
                f"Trade closed - {trade.data._name} "
                f"PnL: {trade.pnl:+.2f}"
            )

    def notify_cashvalue(self, cash, value):
        """More efficient portfolio tracking"""
        # Cache for use in decision recording
        self._cached_cash = cash
        self._cached_value = value

    def _start_decision_point(self, stage: str):
        """Start tracking a new decision point"""
        self.decision_count += 1

        # Reset access tracker for new decision
        if self.access_tracker:
            self.access_tracker.reset()

        # Initialize decision structure
        self._current_decision = {
            'decision_point': self.decision_count,
            'stage': stage,
            'bar_time': bt.num2date(self.strategy.datetime[0]),
            'start_portfolio_value': self.strategy.broker.getvalue(),
            'start_portfolio_cash': self.strategy.broker.getcash(),
        }

        # Reset action tracking
        self._decision_actions = []
        self._action_sequence = 0

    def _end_decision_point(self):
        """Finalize and record the decision point"""
        if not self._current_decision:
            return

        # Determine final action
        if self._decision_actions:
            # Multiple actions - create summary
            action_summary = ' → '.join([
                f"{a['action']}({a['symbol']})"
                for a in sorted(self._decision_actions, key=lambda x: x['seq'])
            ])
        else:
            action_summary = 'HOLD'

        # Get accessed data with field details
        accessed_data = []
        if self.access_tracker:
            accessed_data = self.access_tracker.get_accessed_data()

        # Record the complete decision
        self._record_decision(action_summary, accessed_data)

    def _record_decision(self, action: str, accessed_data: Optional[List] = None):
        """Record a decision with all context"""
        if not self._current_decision:
            return

        # Get current positions
        positions = self._get_positions()

        # Build decision record
        decision = {
            'session_id': self.session_id,
            'agent_id': self.p.agent_id,
            'decision_point': self._current_decision['decision_point'],
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'bar_time': self._current_decision['bar_time'].isoformat(),
            'stage': self._current_decision['stage'],
            'action': action,
            'actions': self._decision_actions,  # Detailed action list
            'portfolio_value': float(self.strategy.broker.getvalue()),
            'portfolio_cash': float(self.strategy.broker.getcash()),
            'positions': positions,
            'data_accessed': accessed_data or [],
            'access_count': self.access_tracker.get_access_count() if self.access_tracker else 0,
        }

        # Add to batch
        self._add_to_batch(decision)

        # Log decision
        logger.debug(
            f"Decision {self.decision_count}: {action} - "
            f"Value: ${decision['portfolio_value']:,.2f}, "
            f"Accessed: {len(decision['data_accessed'])} feeds"
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