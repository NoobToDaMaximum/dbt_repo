{{ config(
  materialized='table',
  target_database='coding-interview-terraform',
  target_schema='data_mart'
) }}

with
  -- CTE to extract and combine all addresses and their values from both inputs and outputs
  all_addresses_and_values as (
    -- Extract values from outputs
    select
      outputs.addresses as address,
      outputs.value as value,
      t.transaction_id as transaction_id
    from {{ ref('stg_transactions') }} as t,
    unnest(t.outputs) as outputs

    union all

    -- Extract values from inputs (treated as negative values for balance calculation)
    select
      inputs.addresses as address,
      -inputs.value as value,
      t.transaction_id as transaction_id
    from {{ ref('stg_transactions') }} as t,
    unnest(t.inputs) as inputs
  ),

  -- CTE to get a list of all transactions involved in a coinbase transaction
  coinbase_transactions as (
    select
      `hash` as transaction_id
    from `bigquery-public-data.crypto_bitcoin_cash.transactions`
    where inputs[offset(0)].is_coinbase is true
  ),

  -- CTE to get a list of all addresses involved in a coinbase transaction
  coinbase_addresses as (
    select distinct
      address
    from all_addresses_and_values
    where transaction_id in (select transaction_id from coinbase_transactions)
  ),

  -- Final balance calculation, excluding coinbase-related addresses
  final_balances as (
    select
      address,
      sum(value) as current_balance
    from all_addresses_and_values
    where address not in (select address from coinbase_addresses)
    group by address
  )

select
  address,
  current_balance
from final_balances