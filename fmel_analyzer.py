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
Decisions are streamed in real-time using BigQuery's insert_rows_json() API
with a ThreadPoolExecutor for non-blocking writes. Each decision includes a
unified event_timeline with all data accesses and trades that map directly to
BigQuery's schema (see bigquery_schema.json).
"""

import backtrader as bt
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional, Dict, List
from google.cloud import bigquery
from google.cloud import pubsub_v1
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

        # Firebase user ID for security rules in Firestore
        # Enables filtering decisions by user on the frontend
        ('user_id', None),

        # Google Cloud project ID for BigQuery and Pub/Sub
        ('project_id', None),

        # BigQuery dataset name - always 'fmel' for Foundation Model Explainability Layer
        ('dataset_id', 'fmel'),

        # BigQuery table name - always 'decisions' (matches Infrastructure/bigquery.tf schema)
        ('table_id', 'decisions'),

        # Pub/Sub topic for real-time decision streaming (e.g., "fmel-decisions")
        # If None, Pub/Sub publishing is disabled (BigQuery-only mode)
        ('pubsub_topic', None),

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

        # ThreadPoolExecutor for non-blocking BigQuery writes
        # Single worker to maintain order and avoid overwhelming BigQuery
        self._executor = ThreadPoolExecutor(max_workers=1)

        # Reference to the AccessTracker that records data field accesses
        # Shared with AccessTrackingWrapper instances that wrap data feeds
        self.access_tracker = self.p.access_tracker

        # =====================================================================
        # PUB/SUB PUBLISHER
        # For real-time streaming to Firestore via Cloud Function.
        # Initialized in start() if pubsub_topic is set.
        # =====================================================================
        self.pubsub_publisher = None
        self.pubsub_topic_path = None

        logger.info(f"FMEL Analyzer initialized - Session: {self.session_id}")

    def start(self):
        """Called when analyzer starts"""
        # Initialize BigQuery client and table reference
        self._setup_bigquery()

        # Initialize Pub/Sub for real-time streaming (if configured)
        if self.p.pubsub_topic:
            self._setup_pubsub()

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
            for access in feed_data.get('access_patterns', []):
                data_hash = access.get('data_hash')

                # Log warning if DATA_ACCESS has no hash (should never happen for valid access)
                if data_hash is None:
                    logger.warning(
                        f"FMEL traceability gap in event_timeline: "
                        f"{symbol}.{access['field']}[{access['index']}] has no data_hash"
                    )

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
                    # Use per-access hash for correct lookback traceability
                    # When agent accesses data.close[-1], this is the hash of THAT bar
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

        # Add to batch (BigQuery)
        self._add_to_batch(decision)

        # Publish to Pub/Sub for real-time streaming (if configured)
        if self.pubsub_publisher:
            self._publish_to_pubsub(decision)

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

    def _setup_pubsub(self):
        """Initialize Pub/Sub publisher for real-time streaming"""
        try:
            self.pubsub_publisher = pubsub_v1.PublisherClient()
            self.pubsub_topic_path = self.pubsub_publisher.topic_path(
                self.p.project_id,
                self.p.pubsub_topic
            )
            logger.info(f"Pub/Sub publisher initialized - Topic: {self.p.pubsub_topic}")
        except Exception as e:
            logger.error(f"Failed to setup Pub/Sub: {e}")
            # Don't raise - Pub/Sub is optional, BigQuery is primary
            self.pubsub_publisher = None
            self.pubsub_topic_path = None

    def _publish_to_pubsub(self, decision: Dict):
        """
        Publish decision to Pub/Sub for real-time streaming.

        The message format is optimized for Firestore:
        - Includes user_id for security rules
        - Flattens nested data for efficient queries
        - Strips data_accessed (too large, not needed for real-time view)
        """
        if not self.pubsub_publisher:
            return

        try:
            # Build optimized message for Firestore (smaller than BigQuery record)
            message = {
                'session_id': decision['session_id'],
                'agent_id': decision['agent_id'],
                'user_id': self.p.user_id,  # Required for Firestore security rules
                'decision_point': decision['decision_point'],
                'timestamp': decision['timestamp'],
                'bar_time': decision['bar_time'],
                'stage': decision['stage'],
                'action': decision['action'],
                'portfolio_value': decision['portfolio_value'],
                'portfolio_cash': decision['portfolio_cash'],
                'access_count': decision['access_count'],
                'positions': decision['positions'],
                # Include full event timeline (trades + data access patterns)
                'event_timeline': [
                    {
                        'seq': e.get('seq', 0),
                        'event_type': e['event_type'],
                        'symbol': e.get('symbol'),
                        'field': e.get('field'),
                        'index': e.get('index'),
                        'action': e.get('action'),
                        'size': e.get('size'),
                        'price': e.get('price'),
                    }
                    for e in decision.get('event_timeline', [])
                ]
            }

            # Serialize and publish
            data = json.dumps(message).encode('utf-8')
            future = self.pubsub_publisher.publish(self.pubsub_topic_path, data)

            # Fire and forget - don't block on publish result
            # Errors will be logged by the publisher's error callback
            future.add_done_callback(self._pubsub_callback)

        except Exception as e:
            logger.warning(f"Failed to publish to Pub/Sub: {e}")
            # Don't raise - Pub/Sub failure shouldn't stop BigQuery writes

    def _pubsub_callback(self, future):
        """Callback for Pub/Sub publish result"""
        try:
            # This will raise if publish failed
            future.result()
        except Exception as e:
            logger.warning(f"Pub/Sub publish failed: {e}")

    def _add_to_batch(self, decision: Dict):
        """Submit decision to BigQuery via ThreadPoolExecutor (non-blocking)."""
        # Submit to executor - returns immediately, write happens in background
        self._executor.submit(self._write_to_bigquery, decision)

    def _write_to_bigquery(self, decision: Dict):
        """Write decision to BigQuery (runs in background thread)."""
        try:
            errors = self.bq_client.insert_rows_json(
                self.table_ref,
                [decision],
                retry=retry.Retry(deadline=30.0)
            )
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
            else:
                logger.info(f"Wrote decision {decision['decision_point']} to BigQuery")
        except Exception as e:
            logger.error(f"Failed to write to BigQuery: {e}")

    def stop(self):
        """Called when analyzer stops"""
        # Record final decision if pending
        if self._current_decision:
            self._end_decision_point()

        # Shutdown executor and wait for pending writes to complete
        if self._executor:
            try:
                self._executor.shutdown(wait=True, cancel_futures=False)
                logger.info("BigQuery executor shutdown complete")
            except Exception as e:
                logger.warning(f"Error shutting down executor: {e}")

        # Log session summary
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