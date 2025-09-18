{{ config(
  materialized='table',
  target_database='coding-interview-terraform',
  target_schema='data_mart'
) }}

WITH
  -- CTE to extract and combine all addresses and their values from both inputs and outputs
  all_addresses_and_values AS (
    -- Extract values from outputs
    SELECT
      unnested_addresses AS `address`,
      outputs.value AS `value`,
      t.transaction_id AS transaction_id
    FROM {{ ref('stg_transactions') }} AS t,
    UNNEST(t.outputs) AS outputs,
    UNNEST(outputs.addresses) AS unnested_addresses

    UNION ALL

    -- Extract values from inputs (treated as negative values for balance calculation)
    SELECT
      unnested_addresses AS `address`,
      -inputs.value AS `value`,
      t.transaction_id AS transaction_id
    FROM {{ ref('stg_transactions') }} AS t,
    UNNEST(t.inputs) AS inputs,
    UNNEST(inputs.addresses) AS unnested_addresses
  ),

  -- CTE to get a list of all transactions involved in a coinbase transaction
  coinbase_transactions AS (
    SELECT
      `hash` AS transaction_id
    FROM `bigquery-public-data.crypto_bitcoin_cash.transactions`
    WHERE is_coinbase IS TRUE
  ),

  -- CTE to get a list of all addresses involved in a coinbase transaction
  coinbase_addresses AS (
    SELECT DISTINCT
      address
    FROM all_addresses_and_values
    WHERE transaction_id IN (SELECT transaction_id FROM coinbase_transactions)
  ),

  -- Final balance calculation, excluding coinbase-related addresses
  final_balances AS (
    SELECT
      `address`,
      SUM(`value`) AS current_balance
    FROM all_addresses_and_values
    WHERE `address` NOT IN (SELECT `address` FROM coinbase_addresses)
    GROUP BY `address`
  )

SELECT
  `address`,
  current_balance
FROM final_balances