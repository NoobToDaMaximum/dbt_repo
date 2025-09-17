{{ config(
  materialized='table'
) }}

SELECT
    `hash` AS transaction_id,
    block_hash,
    block_timestamp,
    inputs,
    outputs,
FROM `bigquery-public-data.crypto_bitcoin_cash.transactions`
WHERE block_timestamp >= cast(timestamp_sub(cast(current_timestamp() AS DATETIME), interval 3 MONTH) AS TIMESTAMP)