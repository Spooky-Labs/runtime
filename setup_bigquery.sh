#!/bin/bash

# FMEL BigQuery Setup Script
# Creates dataset, table, and Pub/Sub subscription for FMEL decision records

set -e

# Configuration
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-the-farm-neutrino-315cd}"
DATASET_ID="fmel"
TABLE_ID="decisions_v2"  # v2 includes field-level access tracking
TOPIC_NAME="fmel-decisions"
SUBSCRIPTION_NAME="fmel-decisions-bigquery"

echo "Setting up FMEL BigQuery integration..."
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET_ID"
echo "Table: $TABLE_ID"
echo ""

# 1. Create BigQuery dataset
echo "Creating BigQuery dataset..."
bq mk --dataset \
  --location=US \
  --description="FMEL (Foundation Model Explainability Layer) decision records for trading agents" \
  ${PROJECT_ID}:${DATASET_ID} 2>/dev/null || echo "Dataset already exists"

# 2. Create BigQuery table with schema
echo "Creating BigQuery table..."
bq mk --table \
  --description="Agent decision records with market data references" \
  --time_partitioning_field=timestamp \
  --time_partitioning_type=DAY \
  --clustering_fields=agent_id,session_id \
  ${PROJECT_ID}:${DATASET_ID}.${TABLE_ID} \
  bigquery_schema.json

echo "✓ Table created with partitioning and clustering"

# 3. Create Pub/Sub → BigQuery subscription
echo "Creating Pub/Sub → BigQuery subscription..."
gcloud pubsub subscriptions create ${SUBSCRIPTION_NAME} \
  --topic=${TOPIC_NAME} \
  --bigquery-table=${PROJECT_ID}:${DATASET_ID}.${TABLE_ID} \
  --use-topic-schema \
  --write-metadata \
  --project=${PROJECT_ID} 2>/dev/null || echo "Subscription already exists"

echo ""
echo "✅ FMEL BigQuery setup complete!"
echo ""
echo "Dashboard Query Examples:"
echo ""
echo "# Recent decisions by agent:"
echo "SELECT agent_id, session_id, bar, action, portfolio_value, timestamp"
echo "FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\`"
echo "WHERE DATE(timestamp) = CURRENT_DATE()"
echo "ORDER BY timestamp DESC"
echo "LIMIT 100"
echo ""
echo "# Performance by session:"
echo "SELECT "
echo "  session_id,"
echo "  agent_id,"
echo "  MIN(timestamp) as session_start,"
echo "  MAX(timestamp) as session_end,"
echo "  MAX(portfolio_value) - MIN(portfolio_value) as pnl,"
echo "  COUNT(*) as total_bars,"
echo "  COUNTIF(action IN ('BUY', 'SELL')) as trade_count"
echo "FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\`"
echo "GROUP BY session_id, agent_id"
echo "ORDER BY session_start DESC"
echo ""
echo "# Action distribution:"
echo "SELECT action, COUNT(*) as count"
echo "FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\`"
echo "WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
echo "GROUP BY action"
echo "ORDER BY count DESC"
