{{ config(
  materialized='table',
  target_database='coding-interview-terraform',
  target_schema='data_mart'
) }}

-- This CTE unnests the 'inputs' and 'outputs' arrays and combines them into a single table.
-- Input values are negated to represent a decrease in balance.
WITH
  all_addresses_and_values AS (
    -- Extract values from outputs
    SELECT
      unnested_addresses AS `address`,
      outputs.value AS `value`,
      t.transaction_id AS transaction_id,
      t.is_coinbase
    FROM {{ ref('stg_transactions') }} AS t,
    UNNEST(t.outputs) AS outputs,
    UNNEST(outputs.addresses) AS unnested_addresses

    UNION ALL

    -- Extract values from inputs
    SELECT
      unnested_addresses AS `address`,
      -inputs.value AS `value`,
      t.transaction_id AS transaction_id,
      t.is_coinbase
    FROM {{ ref('stg_transactions') }} AS t,
    UNNEST(t.inputs) AS inputs,
    UNNEST(inputs.addresses) AS unnested_addresses
  ),

  -- This CTE calculates the final balance for each address.
  final_balances AS (
    SELECT
      `address`,
      SUM(`value`) AS current_balance
    FROM all_addresses_and_values
    WHERE NOT is_coinbase
    GROUP BY `address`
  )

--This final select statement retrieves the address and calculated balance.
SELECT
  `address`,
  current_balance
FROM final_balances