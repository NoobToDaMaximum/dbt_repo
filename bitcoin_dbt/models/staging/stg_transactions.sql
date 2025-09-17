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
WHERE block_timestamp >= timestamp_sub(current_timestamp(), interval 3 MONTH)