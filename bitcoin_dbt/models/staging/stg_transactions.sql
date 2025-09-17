{{ config(
  materialized='table'
) }}

select
    `hash` as transaction_id,
    block_hash,
    block_timestamp,
    inputs,
    outputs,
    -- Add the coinbase flag to the staging table
    inputs[offset(0)].is_coinbase as is_coinbase_related
from `bigquery-public-data.crypto_bitcoin_cash.transactions`
where block_timestamp >= cast(timestamp_sub(cast(current_timestamp() as datetime), interval 3 month) as timestamp)