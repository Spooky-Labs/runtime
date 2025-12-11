#!/usr/bin/env python3
"""
Trading Runtime - Main Orchestrator

This is the entry point for the paper trading platform. It orchestrates all components
needed to run a live trading agent:

1. **Data Feeds**: Stream real-time market data from Google Pub/Sub
2. **Broker**: Execute trades via Alpaca's paper trading API
3. **FMEL Analyzer**: Track all trading decisions for explainability
4. **Agent Strategy**: The trading logic that makes buy/sell decisions

The runtime is designed for production use in Kubernetes with:
- Graceful shutdown on SIGINT/SIGTERM
- Automatic credential management via Secret Manager
- Configurable via environment variables

Typical deployment flow:
  User uploads agent → Cloud Build tests → K8s deployment → This runtime starts
"""

import os
import sys
import signal
import logging
import json
from typing import List

import backtrader as bt
from google.cloud import secretmanager
import redis

# Import custom components - each handles a specific responsibility:
# - data_feed: Streams market data from Pub/Sub into Backtrader format
# - shared_subscriber: Manages shared Pub/Sub subscriptions (reduces quota usage)
# - broker: Translates Backtrader orders to Alpaca API calls
# - agent: Contains the actual trading strategy (replaceable by user code)
# - fmel_analyzer: Records all decisions to BigQuery for explainability
# - access_tracker: Tracks which data fields the agent looked at
# - news_data_feed: Streams news from Pub/Sub into Backtrader format
# - news_shared_subscriber: Manages shared Pub/Sub subscription for news
from data_feed import PubSubMarketDataFeed
from shared_subscriber import SharedSubscriber
from news_data_feed import NewsDataFeed
from news_shared_subscriber import NewsSharedSubscriber
from broker import AlpacaPaperTradingBroker
from agent.agent import Agent
from fmel_analyzer import FMELAnalyzer
from access_tracker import AccessTracker

# Configure logging to stdout for Kubernetes log aggregation.
# Using a simple format that works well with Cloud Logging.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)


class TradingRuntime:
    """
    Main runtime orchestrator for paper trading.

    This class brings together all components needed for live trading:
    - Loads configuration from environment variables
    - Sets up the Backtrader engine (Cerebro)
    - Manages graceful shutdown

    The runtime is stateless - all configuration comes from environment
    variables, making it easy to deploy different agents with different
    settings in Kubernetes.
    """

    def __init__(self):
        # =================================================================
        # CONFIGURATION LOADING
        # Required environment variables must be set by the deployment.
        # In production, these come from K8s ConfigMaps and Secrets.
        # =================================================================
        self.project_id = os.environ['GOOGLE_CLOUD_PROJECT']
        self.agent_id = os.environ['AGENT_ID']
        self.account_id = os.environ['ALPACA_ACCOUNT_ID']
        self.user_id = os.environ.get('USER_ID')  # Firebase user ID for Firestore security

        # Create agent-specific logger to allow filtering logs by agent in
        # Cloud Logging when multiple agents run in the same cluster.
        self.logger = logging.getLogger(f"runtime.{self.agent_id}")

        # Redis connection for symbol discovery
        self.redis_host = os.environ.get('REDIS_HOST', 'localhost')
        self.redis_port = int(os.environ.get('REDIS_PORT', 6379))

        # Connect to Redis
        self.redis_client = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            decode_responses=True
        )

        # Get symbols from Redis (populated by asset-discovery service)
        # We'll fetch both equity and crypto symbols for diversified trading
        self.symbols = self._get_symbols_from_redis()

        # =================================================================
        # CREDENTIAL MANAGEMENT
        # Two-tier credential loading:
        # 1. Check environment variables (for local dev and testing)
        # 2. Fall back to Secret Manager (for production)
        # =================================================================
        self.api_key = os.environ.get('ALPACA_API_KEY')
        self.secret_key = os.environ.get('ALPACA_SECRET_KEY')

        if not self.api_key or not self.secret_key:
            # In production, fetch from Google Secret Manager using Workload Identity.
            # The pod's service account must have secretAccessor permission.
            # Secret names must be provided via environment variables.
            api_key_name = os.environ['RUNTIME_BROKER_API_KEY_NAME']
            secret_key_name = os.environ['RUNTIME_BROKER_SECRET_KEY_NAME']

            self.api_key = self._get_secret(api_key_name)
            self.secret_key = self._get_secret(secret_key_name)

        # FMEL configuration for decision tracking.
        # decisions table stores all trading decisions with field-level access tracking.
        self.fmel_dataset = os.environ.get('FMEL_DATASET', 'fmel')
        self.fmel_table = os.environ.get('FMEL_TABLE', 'decisions')

        # Allow configurable log verbosity for debugging.
        log_level = os.environ.get('LOG_LEVEL', 'INFO')
        self.logger.setLevel(getattr(logging, log_level))

        # =================================================================
        # RUNTIME STATE
        # =================================================================
        self.cerebro = None  # Backtrader engine, created in setup()
        self._shutdown_requested = False

        # Register handlers for graceful shutdown.
        # SIGTERM is sent by Kubernetes during pod termination.
        # SIGINT is sent when user presses Ctrl+C locally.
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _get_symbols_from_redis(self) -> List[str]:
        """
        Get trading symbols from Redis cache (both equities and crypto).

        The asset-discovery service populates Redis with current tradable symbols.
        We fetch both equity and crypto symbols for a diversified trading strategy.

        Returns:
            Combined list of equity and crypto symbols

        Raises:
            SystemExit: If no symbols are found in Redis (critical error)
        """
        all_symbols = []

        try:
            # Fetch equity symbols
            equity_key = "symbols:equities"
            equity_cached = self.redis_client.get(equity_key)

            if equity_cached:
                equity_symbols = json.loads(equity_cached)
                self.logger.info(f"Loaded {len(equity_symbols)} equity symbols from Redis")

                # By default, use ALL available symbols (0 = no limit)
                max_equity = int(os.environ.get('MAX_EQUITY_SYMBOLS', '0'))
                if max_equity > 0:
                    equity_symbols = equity_symbols[:max_equity]
                    self.logger.info(f"Limited to {len(equity_symbols)} equity symbols (MAX_EQUITY_SYMBOLS={max_equity})")
                else:
                    self.logger.info(f"Using ALL {len(equity_symbols)} available equity symbols")

                all_symbols.extend(equity_symbols)
            else:
                self.logger.warning(f"No equity symbols found at key '{equity_key}'")

            # Fetch crypto symbols
            crypto_key = "symbols:crypto"
            crypto_cached = self.redis_client.get(crypto_key)

            if crypto_cached:
                crypto_symbols = json.loads(crypto_cached)
                self.logger.info(f"Loaded {len(crypto_symbols)} crypto symbols from Redis")

                # By default, use ALL available symbols (0 = no limit)
                max_crypto = int(os.environ.get('MAX_CRYPTO_SYMBOLS', '0'))
                if max_crypto > 0:
                    crypto_symbols = crypto_symbols[:max_crypto]
                    self.logger.info(f"Limited to {len(crypto_symbols)} crypto symbols (MAX_CRYPTO_SYMBOLS={max_crypto})")
                else:
                    self.logger.info(f"Using ALL {len(crypto_symbols)} available crypto symbols")

                all_symbols.extend(crypto_symbols)
            else:
                self.logger.warning(f"No crypto symbols found at key '{crypto_key}'")

            # Check if we have any symbols at all
            if not all_symbols:
                error_msg = f"""No symbols found in Redis!

The asset-discovery service must populate Redis before the runtime can start.
Please ensure:
  1. Redis is accessible at {self.redis_host}:{self.redis_port}
  2. The asset-discovery services have run successfully
  3. The cache keys 'symbols:equities' and 'symbols:crypto' contain valid data

To run asset discovery manually:
  kubectl create job --from=cronjob/equity-asset-refresh equity-manual -n paper-trading
  kubectl create job --from=cronjob/crypto-asset-refresh crypto-manual -n paper-trading"""

                self.logger.error(error_msg)
                sys.exit(1)

            self.logger.info(f"Total symbols to trade: {len(all_symbols)} (equities + crypto)")
            return all_symbols

        except redis.ConnectionError as e:
            self.logger.error(f"Failed to connect to Redis at {self.redis_host}:{self.redis_port}: {e}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in Redis cache: {e}")
            sys.exit(1)
        except Exception as e:
            self.logger.error(f"Unexpected error getting symbols from Redis: {e}")
            sys.exit(1)

    def _get_secret(self, name: str) -> str:
        """
        Fetch a secret from Google Secret Manager.

        Uses Application Default Credentials (ADC), which automatically works with:
        - Workload Identity in GKE (production)
        - gcloud auth application-default login (local development)
        - Service account key file (legacy)
        """
        client = secretmanager.SecretManagerServiceClient()
        path = f"projects/{self.project_id}/secrets/{name}/versions/latest"
        return client.access_secret_version(request={"name": path}).payload.data.decode('UTF-8')

    def _signal_handler(self, signum, frame):
        """
        Handle termination signals for graceful shutdown.

        When Kubernetes sends SIGTERM, we want to:
        1. Stop accepting new data
        2. Cancel pending orders (if desired)
        3. Flush FMEL batches to BigQuery
        4. Clean up resources
        """
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self._shutdown_requested = True
        # Note: Backtrader's Cerebro doesn't have a stop method.
        # Cleanup happens after run() returns in _cleanup().

    def setup(self):
        """
        Initialize the Backtrader engine with all components.

        This sets up the complete trading pipeline:
        1. Data feeds for each symbol (connected to Pub/Sub)
        2. The Agent strategy (trading logic)
        3. Alpaca broker for order execution
        4. FMEL analyzer for decision tracking

        Must be called before run().
        """
        # stdstats=False disables default observers (cash, value, trades)
        # We use FMEL analyzer instead for more detailed tracking.
        self.cerebro = bt.Cerebro(stdstats=False)

        # Create access tracker to record which data fields the agent accesses.
        # This enables complete explainability - we can trace exactly what
        # data the agent "looked at" before making each decision.
        access_tracker = AccessTracker()

        # =====================================================================
        # DATA FEEDS
        # All feeds share 2 Pub/Sub subscriptions (one per topic) via SharedSubscriber.
        # Symbol format determines topic: "/" means crypto (24/7), else equity.
        #
        # This pattern reduces subscriptions from O(symbols) to O(topics) per agent:
        # - Old pattern: 70 symbols × 100 agents = 7,000 subscriptions (approaching 10k limit)
        # - New pattern: 2 topics × 100 agents = 200 subscriptions (supports 5,000 agents)
        # =====================================================================
        data_feeds = []
        for symbol in self.symbols:
            # Route to appropriate Pub/Sub topic based on asset type.
            # The data-ingestor service publishes to these topics.
            topic = 'crypto-data' if '/' in symbol else 'market-data'

            feed = PubSubMarketDataFeed(
                project_id=self.project_id,
                topic_name=topic,
                symbol=symbol,
                agent_id=self.agent_id,  # Used for subscription naming
                buffer_size=1000  # Buffer 1000 bars to handle processing delays
            )

            # Register feed with Backtrader. The name is used for position tracking.
            self.cerebro.adddata(feed, name=symbol)
            data_feeds.append(feed)
            self.logger.debug(f"Added feed: {symbol} → {topic}")

        # =====================================================================
        # NEWS DATA FEEDS
        # One feed per news source. Agent accesses via:
        #   news = self.getdatabyname('ALPACA_NEWS')
        # =====================================================================
        news_feeds = []

        # Alpaca News Feed
        alpaca_news_feed = NewsDataFeed(
            project_id=self.project_id,
            source_name='ALPACA',
            agent_id=self.agent_id,
            buffer_size=500
        )
        self.cerebro.adddata(alpaca_news_feed, name='ALPACA_NEWS')
        news_feeds.append(alpaca_news_feed)
        self.logger.info("Added news feed: ALPACA_NEWS")

        # Future: Add other news sources here
        # bloomberg_news_feed = NewsDataFeed(project_id=..., source_name='BLOOMBERG', ...)
        # self.cerebro.adddata(bloomberg_news_feed, name='BLOOMBERG_NEWS')

        # =====================================================================
        # STRATEGY
        # Add the trading strategy. This is imported from agent/agent.py and
        # contains the actual trading logic (when to buy/sell).
        # =====================================================================
        self.cerebro.addstrategy(Agent)

        # =====================================================================
        # BROKER
        # Connect to Alpaca for order execution. sandbox=True means we use
        # paper trading - no real money is involved.
        # =====================================================================
        broker = AlpacaPaperTradingBroker(
            api_key=self.api_key,
            secret_key=self.secret_key,
            account_id=self.account_id,
            sandbox=True,  # Always paper trading - this is a simulation platform
            poll_interval=2.0  # Check order status every 2 seconds
        )
        self.cerebro.setbroker(broker)

        # =====================================================================
        # FMEL ANALYZER
        # Foundation Model Explainability Layer - records every decision to
        # BigQuery with full context: what data was accessed, what action was
        # taken, and portfolio state before/after.
        # =====================================================================
        self.cerebro.addanalyzer(
            FMELAnalyzer,
            agent_id=self.agent_id,
            user_id=self.user_id,  # For Firestore security rules
            project_id=self.project_id,
            dataset_id=self.fmel_dataset,
            table_id=self.fmel_table,
            pubsub_topic='fmel-decisions',  # Real-time streaming to Firestore
            access_tracker=access_tracker,
            data_feeds=data_feeds + news_feeds  # Include news in FMEL tracking
        )

    def run(self):
        """
        Execute the trading runtime.

        This is the main loop that:
        1. Starts receiving market data from Pub/Sub
        2. Feeds data to the strategy bar-by-bar
        3. Executes orders through the broker
        4. Records decisions to FMEL

        The loop runs until:
        - The data feeds are exhausted (won't happen with live data)
        - An error occurs
        - A shutdown signal is received

        Returns:
            bool: True if the session was profitable or break-even
        """
        if not self.cerebro:
            raise RuntimeError("Runtime not setup. Call setup() first.")

        # Log initial state for debugging
        self.logger.info(
            f"Portfolio initialized - Starting cash: ${self.cerebro.broker.getcash():,.2f}"
        )

        try:
            # live=True tells Backtrader this is a live data feed, not historical.
            # This affects how it handles data timing and preloading.
            self.cerebro.run(live=True)
            self.logger.info("Trading engine started successfully")

        except KeyboardInterrupt:
            self.logger.info("Trading interrupted by user")
        except Exception as e:
            self.logger.error(f"Trading engine error: {e}", exc_info=True)
            raise
        finally:
            # Always clean up resources, even on error.
            self._cleanup()

        # Log shutdown
        self.logger.info("Runtime stopped - Performance metrics available via Alpaca API")

        # Return True for successful run (used for CI/CD pass/fail).
        return True

    def _cleanup(self):
        """
        Clean up all resources on shutdown.

        This is called automatically after run() completes, ensuring:
        - Broker stops monitoring orders (stops background thread)
        - Data feeds unregister from SharedSubscriber
        - SharedSubscriber deletes Pub/Sub subscriptions
        - FMEL analyzer flushes remaining decisions to BigQuery

        Important: Analyzer stop() is called automatically by Backtrader.
        """
        # Stop broker's background order monitoring thread.
        if hasattr(self.cerebro.broker, 'stop'):
            try:
                self.cerebro.broker.stop()
            except Exception as e:
                self.logger.error(f"Error stopping broker: {e}")

        # Stop data feeds - this unregisters from SharedSubscriber.
        for data in self.cerebro.datas:
            if hasattr(data, 'stop'):
                try:
                    data.stop()
                except Exception as e:
                    self.logger.error(f"Error stopping feed {data._name}: {e}")

        # Clean up SharedSubscriber instances - this deletes the Pub/Sub subscriptions.
        # This prevents orphaned subscriptions when the pod terminates.
        try:
            SharedSubscriber.cleanup_all()
            self.logger.info("SharedSubscriber cleanup complete")
        except Exception as e:
            self.logger.error(f"Error cleaning up SharedSubscriber: {e}")

        # Clean up NewsSharedSubscriber instances - this deletes the news Pub/Sub subscriptions.
        try:
            NewsSharedSubscriber.cleanup_all()
            self.logger.info("NewsSharedSubscriber cleanup complete")
        except Exception as e:
            self.logger.error(f"Error cleaning up NewsSharedSubscriber: {e}")

        # Note: Analyzer.stop() is called automatically by Backtrader's run().
        # This triggers FMEL to flush any remaining batched decisions.


def main():
    """
    Entry point for the trading runtime.

    Exit codes:
        0: Successful run (profitable or break-even)
        1: Successful run but unprofitable
        2: Runtime error/exception
    """
    try:
        runtime = TradingRuntime()
        runtime.logger.info(f"Runtime started - Symbols: {runtime.symbols}")

        runtime.setup()
        success = runtime.run()
        sys.exit(0 if success else 1)

    except Exception as e:
        logging.getLogger('runtime').error(f"Runtime failed: {e}", exc_info=True)
        sys.exit(2)


if __name__ == "__main__":
    main()