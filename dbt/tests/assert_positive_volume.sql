-- Singular test: assert that every row in fact_daily_prices has volume > 0.
-- Returns rows that FAIL the assertion (dbt expects 0 rows from a passing test).

SELECT
    ticker,
    trade_date,
    volume
FROM {{ ref('fact_daily_prices') }}
WHERE volume <= 0
