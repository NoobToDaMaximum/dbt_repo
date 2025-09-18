{{ config(
  materialized='table',
  partition_by={
    'field': 'block_timestamp',
    'data_type': 'timestamp'
  }
) }}

WITH max_block_timestamp AS (
  SELECT
    max(block_timestamp) AS latest_timestamp
  FROM `bigquery-public-data.crypto_bitcoin_cash.transactions`
)

SELECT
    `hash` AS transaction_id,
    block_hash,
    block_timestamp,
    is_coinbase,
    (SELECT ARRAY_AGG(STRUCT(addresses, `value`)) FROM UNNEST(inputs) AS i) AS inputs,
    (SELECT ARRAY_AGG(STRUCT(addresses, `value`)) FROM UNNEST(outputs) AS o) AS outputs
FROM `bigquery-public-data.crypto_bitcoin_cash.transactions`
WHERE block_timestamp >= (
  SELECT timestamp_sub(latest_timestamp, interval 3 * 30 day)
  FROM max_block_timestamp
)