#!/usr/bin/env python3
"""
Component Tests for Trading Runtime
Tests individual components in isolation
"""

import os
import sys

# Add parent directory to path so we can import runtime modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test_components')


def test_imports():
    """Test that all modules can be imported"""
    logger.info("Testing imports...")

    try:
        import backtrader as bt
        logger.info("  - backtrader: OK")
    except ImportError as e:
        logger.error(f"  - backtrader: FAILED - {e}")
        return False

    try:
        from broker import AlpacaPaperTradingBroker
        logger.info("  - broker: OK")
    except ImportError as e:
        logger.error(f"  - broker: FAILED - {e}")
        return False

    try:
        from data_feed import PubSubMarketDataFeed
        logger.info("  - data_feed: OK")
    except ImportError as e:
        logger.error(f"  - data_feed: FAILED - {e}")
        return False

    try:
        from fmel_analyzer import FMELAnalyzer
        logger.info("  - fmel_analyzer: OK")
    except ImportError as e:
        logger.error(f"  - fmel_analyzer: FAILED - {e}")
        return False

    try:
        from access_tracker import AccessTracker, AccessTrackingWrapper
        logger.info("  - access_tracker: OK")
    except ImportError as e:
        logger.error(f"  - access_tracker: FAILED - {e}")
        return False

    try:
        from agent.agent import Agent
        logger.info("  - agent: OK")
    except ImportError as e:
        logger.error(f"  - agent: FAILED - {e}")
        return False

    try:
        from google.cloud import bigquery, pubsub_v1
        logger.info("  - google.cloud: OK")
    except ImportError as e:
        logger.error(f"  - google.cloud: FAILED - {e}")
        return False

    return True


def test_broker_connection():
    """Test broker can connect and retrieve account info"""
    logger.info("Testing broker connection...")

    api_key = os.environ.get('ALPACA_API_KEY')
    secret_key = os.environ.get('ALPACA_SECRET_KEY')
    account_id = os.environ.get('ALPACA_ACCOUNT_ID')

    if not all([api_key, secret_key, account_id]):
        logger.error("  - Missing Alpaca credentials")
        return False

    try:
        from broker import AlpacaPaperTradingBroker

        broker = AlpacaPaperTradingBroker(
            api_key=api_key,
            secret_key=secret_key,
            account_id=account_id,
            sandbox=True
        )

        cash = broker.getcash()
        value = broker.getvalue()

        logger.info(f"  - Cash: ${cash:,.2f}")
        logger.info(f"  - Value: ${value:,.2f}")

        if value <= 0:
            logger.error("  - Account value is 0 or negative")
            return False

        # Stop broker thread
        broker.stop()

        logger.info("  - Broker connection: OK")
        return True

    except Exception as e:
        logger.error(f"  - Broker connection: FAILED - {e}")
        return False


def test_bigquery_connection():
    """Test BigQuery client can connect"""
    logger.info("Testing BigQuery connection...")

    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
    dataset_id = os.environ.get('FMEL_DATASET', 'fmel')
    table_id = os.environ.get('FMEL_TABLE', 'decisions_v2')

    if not project_id:
        logger.error("  - Missing GOOGLE_CLOUD_PROJECT")
        return False

    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=project_id)
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        # Try to get table info
        table = client.get_table(table_ref)
        logger.info(f"  - Table: {table.full_table_id}")
        logger.info(f"  - Rows: {table.num_rows}")

        logger.info("  - BigQuery connection: OK")
        return True

    except Exception as e:
        logger.error(f"  - BigQuery connection: FAILED - {e}")
        return False


def test_pubsub_connection():
    """Test Pub/Sub client can list topics"""
    logger.info("Testing Pub/Sub connection...")

    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')

    if not project_id:
        logger.error("  - Missing GOOGLE_CLOUD_PROJECT")
        return False

    try:
        from google.cloud import pubsub_v1

        publisher = pubsub_v1.PublisherClient()
        project_path = f"projects/{project_id}"

        # List topics to verify connection
        topics = list(publisher.list_topics(request={"project": project_path}))

        # Check for crypto-data topic
        crypto_topic = f"projects/{project_id}/topics/crypto-data"
        topic_names = [t.name for t in topics]

        if crypto_topic in topic_names:
            logger.info(f"  - Found crypto-data topic")
        else:
            logger.warning(f"  - crypto-data topic not found (available: {len(topics)} topics)")

        logger.info("  - Pub/Sub connection: OK")
        return True

    except Exception as e:
        logger.error(f"  - Pub/Sub connection: FAILED - {e}")
        return False


def test_access_tracker():
    """Test access tracker functionality"""
    logger.info("Testing access tracker...")

    try:
        from access_tracker import AccessTracker

        tracker = AccessTracker()

        # Record some accesses
        tracker.record_access('BTC/USD', 'close', 0, 'hash123')
        tracker.record_access('BTC/USD', 'high', 0, 'hash123')
        tracker.record_access('ETH/USD', 'close', -1, 'hash456')

        # Get accessed data
        accessed = tracker.get_accessed_data()

        if len(accessed) != 2:
            logger.error(f"  - Expected 2 feeds, got {len(accessed)}")
            return False

        # Check access count
        if tracker.get_access_count() != 3:
            logger.error(f"  - Expected 3 accesses, got {tracker.get_access_count()}")
            return False

        logger.info(f"  - Tracked {len(accessed)} feeds, {tracker.get_access_count()} accesses")
        logger.info("  - Access tracker: OK")
        return True

    except Exception as e:
        logger.error(f"  - Access tracker: FAILED - {e}")
        return False


def test_chronos_model():
    """Test Chronos model can be loaded"""
    logger.info("Testing Chronos model loading...")

    try:
        from chronos import ChronosPipeline
        import torch

        # Load the model - using dtype (not torch_dtype) for CPU compatibility
        pipeline = ChronosPipeline.from_pretrained(
            "amazon/chronos-t5-small",
            device_map="cpu",
            dtype=torch.float32
        )

        logger.info("  - Chronos model loaded successfully")
        logger.info("  - Chronos model: OK")
        return True

    except Exception as e:
        logger.error(f"  - Chronos model: FAILED - {e}")
        return False


def main():
    """Run all component tests"""
    logger.info("=" * 60)
    logger.info("Trading Runtime Component Tests")
    logger.info("=" * 60)

    results = {}

    # Run tests
    results['imports'] = test_imports()
    results['broker'] = test_broker_connection()
    results['bigquery'] = test_bigquery_connection()
    results['pubsub'] = test_pubsub_connection()
    results['access_tracker'] = test_access_tracker()
    results['chronos'] = test_chronos_model()

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
        logger.info("\nAll component tests passed!")
        sys.exit(0)
    else:
        logger.error(f"\n{total - passed} tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
