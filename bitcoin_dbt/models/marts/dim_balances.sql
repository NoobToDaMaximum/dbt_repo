{{ config(
  materialized='table',
  target_database='coding-interview-terraform',
  target_schema='data_mart'
) }}

with
  -- Extract and combine all addresses and their values from both inputs and outputs
  all_addresses_and_values as (
    -- Extract values from outputs
    select
      outputs.addresses as address,
      outputs.value as value,
      t.transaction_id as transaction_id,
      t.is_coinbase_related
    from {{ ref('stg_transactions') }} as t,
    unnest(t.outputs) as outputs

    union all

    -- Extract values from inputs (treated as negative values for balance calculation)
    select
      inputs.addresses as address,
      -inputs.value as value,
      t.transaction_id as transaction_id,
      t.is_coinbase_related
    from {{ ref('stg_transactions') }} as t,
    unnest(t.inputs) as inputs
  ),

  -- Final balance calculation, excluding coinbase-related addresses
  final_balances as (
    select
      address,
      sum(value) as current_balance
    from all_addresses_and_values
    where not is_coinbase_related
    group by address
  )

select
  address,
  current_balance
from final_balances