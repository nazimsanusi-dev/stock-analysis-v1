-- dim_company: slowly-changing dimension (Type 2) for company attributes.
-- Sourced from the dbt snapshot `company_snapshot` which tracks historical
-- changes to sector, industry, market_cap_category, and exchange.
-- Each row represents one *version* of a company's attributes.
-- Use `is_current = true` to get today's values.

{{ config(materialized='table') }}

WITH snapshot AS (
    SELECT * FROM {{ ref('company_snapshot') }}
),

-- Classify market cap into buckets that drive the SCD2 change detection
with_cap_category AS (
    SELECT *,
        CASE
            WHEN market_cap >= 200e9 THEN 'Mega Cap'
            WHEN market_cap >= 10e9  THEN 'Large Cap'
            WHEN market_cap >= 2e9   THEN 'Mid Cap'
            WHEN market_cap >= 300e6 THEN 'Small Cap'
            WHEN market_cap IS NOT NULL THEN 'Micro Cap'
            ELSE 'Unknown'
        END AS market_cap_category
    FROM snapshot
)

SELECT
    -- Surrogate key: unique per ticker + version window
    {{ dbt_utils.generate_surrogate_key(['ticker', 'dbt_scd_id']) }}
                                                AS company_sk,
    ticker,
    company_name,
    COALESCE(sector,   'Unknown')               AS sector,
    COALESCE(industry, 'Unknown')               AS industry,
    COALESCE(country,  'Unknown')               AS country,
    COALESCE(exchange, 'Unknown')               AS exchange,
    COALESCE(currency, 'USD')                   AS currency,
    market_cap,
    market_cap_category,
    employees,
    website,

    -- SCD Type 2 validity window
    dbt_valid_from                              AS valid_from,
    dbt_valid_to                                AS valid_to,
    dbt_valid_to IS NULL                        AS is_current,
    dbt_updated_at                              AS updated_at
FROM with_cap_category
