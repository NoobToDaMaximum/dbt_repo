{{ config(
  materialized='table'
) }}

SELECT
    `hash` AS transaction_id,
    block_hash,
    block_timestamp,
    is_coinbase,
    inputs,
    outputs
FROM `bigquery-public-data.crypto_bitcoin_cash.transactions`
WHERE block_timestamp >= CAST(TIMESTAMP_SUB(CAST(current_timestamp() AS DATETIME), INTERVAL 3 MONTH) AS TIMESTAMP)