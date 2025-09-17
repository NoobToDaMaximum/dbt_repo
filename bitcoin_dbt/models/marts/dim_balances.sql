{{ config(
  materialized='table',
  target_database='coding-interview-terraform',
  target_schema='data_mart'
) }}

with
  -- Extract and combine all addresses and their values from both inputs and outputs
  all_addresses_and_values AS (
    -- Extract values from outputs
    SELECT
      outputs.addresses AS `address`,
      outputs.value AS `value`,
      t.transaction_id AS transaction_id
    FROM {{ ref('stg_transactions') }} AS t,
    unnest(t.outputs) AS outputs

    UNION ALL

    -- Extract values FROM inputs (treated AS negative values for balance calculation)
    SELECT
      inputs.addresses AS `address`,
      -inputs.value AS `value`,
      t.transaction_id AS transaction_id
    FROM {{ ref('stg_transactions') }} AS t,
    unnest(t.inputs) AS inputs
  ),

  -- Get a list of all transactions ids involved in a coinbase transaction
  coinbase_transactions AS (
    SELECT
      `hash` AS transaction_id
    FROM `bigquery-public-data.crypto_bitcoin_cash.transactions`
    WHERE t.is_coinbase IS true
  ),

  -- Get a list of all addresses involved in a coinbase transaction
  coinbase_addresses AS (
    SELECT DISTINCT
      `address`
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