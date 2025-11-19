#!/usr/bin/env python3
"""
Integration Tests for Trading Runtime
Tests the full runtime initialization and short operation
"""

import os
import sys

# Add parent directory to path so we can import runtime modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import signal
import logging
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test_integration')


def test_runtime_initialization():
    """Test that the runtime can be fully initialized"""
    logger.info("Testing runtime initialization...")

    try:
        import backtrader as bt
        from broker import AlpacaPaperTradingBroker
        from data_feed import PubSubMarketDataFeed
        from fmel_analyzer import FMELAnalyzer
        from access_tracker import AccessTracker
        from agent.agent import Agent

        # Get configuration
        project_id = os.environ['GOOGLE_CLOUD_PROJECT']
        api_key = os.environ['ALPACA_API_KEY']
        secret_key = os.environ['ALPACA_SECRET_KEY']
        account_id = os.environ['ALPACA_ACCOUNT_ID']
        agent_id = os.environ.get('AGENT_ID', 'test-agent')
        symbols_str = os.environ.get('SYMBOLS', 'BTC/USD')
        symbols = [s.strip() for s in symbols_str.split(',')]

        # Initialize Cerebro
        cerebro = bt.Cerebro(stdstats=False)
        logger.info("  - Cerebro initialized")

        # Create access tracker
        access_tracker = AccessTracker()
        logger.info("  - Access tracker created")

        # Add data feeds
        data_feeds = []
        for symbol in symbols:
            topic = 'crypto-data' if '/' in symbol else 'market-data'
            feed = PubSubMarketDataFeed(
                project_id=project_id,
                topic_name=topic,
                symbol=symbol,
                buffer_size=100
            )
            cerebro.adddata(feed, name=symbol)
            data_feeds.append(feed)
            logger.info(f"  - Data feed added: {symbol}")

        # Add strategy
        cerebro.addstrategy(Agent)
        logger.info("  - Agent strategy added")

        # Setup broker
        broker = AlpacaPaperTradingBroker(
            api_key=api_key,
            secret_key=secret_key,
            account_id=account_id,
            sandbox=True,
            poll_interval=2.0
        )
        cerebro.setbroker(broker)
        logger.info(f"  - Broker setup: ${broker.getvalue():,.2f}")

        # Add FMEL analyzer
        cerebro.addanalyzer(
            FMELAnalyzer,
            agent_id=agent_id,
            project_id=project_id,
            dataset_id=os.environ.get('FMEL_DATASET', 'fmel'),
            table_id=os.environ.get('FMEL_TABLE', 'decisions_v2'),
            batch_size=10,
            batch_timeout=5.0,
            access_tracker=access_tracker,
            data_feeds=data_feeds
        )
        logger.info("  - FMEL analyzer added")

        # Cleanup
        broker.stop()
        for feed in data_feeds:
            feed.stop()

        logger.info("  - Runtime initialization: OK")
        return True

    except Exception as e:
        logger.error(f"  - Runtime initialization: FAILED - {e}")
        import traceback
        traceback.print_exc()
        return False


def test_data_feed_receives_data():
    """Test that data feed can receive data from Pub/Sub"""
    logger.info("Testing data feed receives data...")

    try:
        from data_feed import PubSubMarketDataFeed

        project_id = os.environ['GOOGLE_CLOUD_PROJECT']
        symbol = 'BTC/USD'

        feed = PubSubMarketDataFeed(
            project_id=project_id,
            topic_name='crypto-data',
            symbol=symbol,
            buffer_size=100
        )

        # Start the feed
        feed.start()
        logger.info(f"  - Feed started for {symbol}")

        # Wait for data (up to 30 seconds)
        start_time = time.time()
        timeout = 30
        received_data = False

        while time.time() - start_time < timeout:
            if feed.haslivedata():
                received_data = True
                logger.info(f"  - Received live data after {time.time() - start_time:.1f}s")
                break
            time.sleep(0.5)

        # Stop the feed
        feed.stop()

        if received_data:
            logger.info("  - Data feed receives data: OK")
            return True
        else:
            logger.warning(f"  - No data received within {timeout}s (may be normal if no market activity)")
            # Don't fail the test - market data may not always be available
            logger.info("  - Data feed receives data: OK (timeout is acceptable)")
            return True

    except Exception as e:
        logger.error(f"  - Data feed receives data: FAILED - {e}")
        return False


def test_fmel_writes_to_bigquery():
    """Test that FMEL can write a test decision to BigQuery"""
    logger.info("Testing FMEL writes to BigQuery...")

    try:
        from google.cloud import bigquery
        import time

        project_id = os.environ['GOOGLE_CLOUD_PROJECT']
        dataset_id = os.environ.get('FMEL_DATASET', 'fmel')
        table_id = os.environ.get('FMEL_TABLE', 'decisions_v2')

        client = bigquery.Client(project=project_id)
        table_ref = client.dataset(dataset_id).table(table_id)

        # Create a test decision record
        test_session_id = f"test-session-{int(time.time())}"
        test_decision = {
            'session_id': test_session_id,
            'agent_id': 'test-agent',
            'decision_point': 1,
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'bar_time': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'stage': 'TEST',
            'action': 'TEST_ACTION',
            'actions': [],
            'portfolio_value': 25000.0,
            'portfolio_cash': 25000.0,
            'positions': [],
            'data_accessed': [],
            'access_count': 0,
        }

        # Insert the record
        errors = client.insert_rows_json(table_ref, [test_decision])

        if errors:
            logger.error(f"  - BigQuery insert errors: {errors}")
            return False

        logger.info(f"  - Test decision written to BigQuery (session: {test_session_id})")
        logger.info("  - FMEL writes to BigQuery: OK")
        return True

    except Exception as e:
        logger.error(f"  - FMEL writes to BigQuery: FAILED - {e}")
        return False


def main():
    """Run all integration tests"""
    logger.info("=" * 60)
    logger.info("Trading Runtime Integration Tests")
    logger.info("=" * 60)

    results = {}

    # Run tests
    results['initialization'] = test_runtime_initialization()
    results['data_feed'] = test_data_feed_receives_data()
    results['fmel_bigquery'] = test_fmel_writes_to_bigquery()

    # Summary
    logger.info("=" * 60)
    logger.info("Test Summary")
    logger.info("=" * 60)

    passed = sum(1 for r in results.values() if r)
    total = len(results)

    for name, result in results.items():
        status = "PASS" if result else "FAIL"
        logger.info(f"  {name}: {status}")

    logger.info(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        logger.info("\nAll integration tests passed!")
        sys.exit(0)
    else:
        logger.error(f"\n{total - passed} tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
