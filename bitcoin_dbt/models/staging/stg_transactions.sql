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
where block_timestamp >= timestamp_sub(cast(current_timestamp() as datetime), interval 3 month)