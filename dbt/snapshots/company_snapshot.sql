{% snapshot company_snapshot %}

{{
    config(
        target_schema  = 'snapshots',
        unique_key     = 'ticker',
        strategy       = 'check',
        check_cols     = ['sector', 'industry', 'market_cap_category', 'exchange', 'currency'],
        invalidate_hard_deletes = True,
    )
}}

-- Snapshot source: add the market_cap_category bucket here so changes
-- in bucket membership (e.g. Large Cap → Mega Cap) also trigger a new SCD row.
SELECT
    ticker,
    company_name,
    sector,
    industry,
    country,
    exchange,
    COALESCE(currency, 'USD') AS currency,
    market_cap,
    CASE
        WHEN market_cap >= 200e9 THEN 'Mega Cap'
        WHEN market_cap >= 10e9  THEN 'Large Cap'
        WHEN market_cap >= 2e9   THEN 'Mid Cap'
        WHEN market_cap >= 300e6 THEN 'Small Cap'
        WHEN market_cap IS NOT NULL THEN 'Micro Cap'
        ELSE 'Unknown'
    END AS market_cap_category,
    employees,
    website,
    _extracted_at
FROM {{ ref('stg_company_info') }}

{% endsnapshot %}
