{{ config(
  materialized='table'
) }}

with max_block_timestamp as (
  select
    max(block_timestamp) as latest_timestamp
  from `bigquery-public-data.crypto_bitcoin_cash.transactions`
)

select
    `hash` as transaction_id,
    block_hash,
    block_timestamp,
    inputs,
    outputs,
    is_coinbase
from `bigquery-public-data.crypto_bitcoin_cash.transactions`
where block_timestamp >= (
  select timestamp_sub(latest_timestamp, interval 3 * 30 day)
  from max_block_timestamp
)