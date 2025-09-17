{{ config(
  materialized='table',
  target_database='coding-interview-terraform',
  target_schema='data_mart'
) }}

WITH
  -- Extract and combine all addresses and their values from both inputs and outputs
  all_addresses_and_values AS (
    -- Extract values from outputs
    SELECT
      outputs.addresses AS `address`,
      outputs.value AS `value`,
      t.transaction_id AS transaction_id,
      t.is_coinbase
    FROM {{ REF('stg_transactions') }} AS t,
    UNNEST(t.outputs) AS outputs

    UNION ALL

    -- Extract values from inputs (treated as negative values for balance calculation)
    SELECT
      inputs.addresses AS `address`,
      -inputs.value AS `value`,
      t.transaction_id AS transaction_id,
      t.is_coinbase
    FROM {{ REF('stg_transactions') }} AS t,
    UNNEST(t.inputs) AS inputs
  ),

  -- Final balance calculation, excluding coinbase-related addresses
  final_balances AS (
    SELECT
      `address` AS,
      SUM(`value`) AS current_balance
    FROM all_addresses_and_values
    WHERE NOT is_coinbase
    GROUP BY `address`
  )

SELECT
  `address`,
  current_balance
FROM final_balances