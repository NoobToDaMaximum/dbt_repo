-- The table is partitioned by 'block_timestamp' to enable efficient queries and reduce costs.
-- This allows BigQuery to scan only the relevant partitions instead of the entire table.
{{ config(
  materialized='table',
  partition_by={
    'field': 'block_timestamp',
    'data_type': 'timestamp'
  }
) }}

-- This CTE finds the most recent timestamp in the entire dataset.
WITH max_block_timestamp AS (
  SELECT
    max(block_timestamp) AS latest_timestamp
  FROM `bigquery-public-data.crypto_bitcoin_cash.transactions`
)

-- The main query extracts and processes transaction data from the public Bitcoin Cash dataset.
-- It focuses on the last 3 months of data to optimize performance and cost.
SELECT
    `hash` AS transaction_id,
    block_hash,
    block_timestamp,
    is_coinbase,
    -- These subqueries retrieve only the addresses and value from the inputs and outputs arrays,
    -- which reduces the amount of data processed and stored.
    (SELECT ARRAY_AGG(STRUCT(addresses, `value`)) FROM UNNEST(inputs) AS i) AS inputs,
    (SELECT ARRAY_AGG(STRUCT(addresses, `value`)) FROM UNNEST(outputs) AS o) AS outputs
FROM `bigquery-public-data.crypto_bitcoin_cash.transactions`
WHERE block_timestamp >= (
  SELECT timestamp_sub(latest_timestamp, interval 3 * 30 day)
  FROM max_block_timestamp
)