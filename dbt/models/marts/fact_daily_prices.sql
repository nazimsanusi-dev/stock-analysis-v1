-- fact_daily_prices: central fact table in the star schema.
-- Grain: one row per ticker per trading day.
-- Foreign keys → dim_date (date_sk) and dim_company (company_sk).
-- Materialised incrementally: on each run only appends rows with
-- trade_date > the current maximum in the table.

{{
  config(
    materialized  = 'incremental',
    unique_key    = 'price_sk',
    on_schema_change = 'fail',
    incremental_strategy = 'delete+insert'
  )
}}

WITH returns AS (
    SELECT * FROM {{ ref('int_daily_returns') }}
),

dim_date AS (
    SELECT date_sk, full_date FROM {{ ref('dim_date') }}
),

-- Current company version only (to resolve the FK without duplicates)
dim_company AS (
    SELECT company_sk, ticker
    FROM {{ ref('dim_company') }}
    WHERE is_current = true
),

joined AS (
    SELECT
        -- Surrogate key (stable hash of natural key)
        {{ dbt_utils.generate_surrogate_key(['r.ticker', 'r.trade_date']) }}
                                                AS price_sk,

        -- Foreign keys
        dd.date_sk                              AS date_sk,
        dc.company_sk                           AS company_sk,

        -- Degenerate dimensions (kept on fact for convenience)
        r.ticker,
        r.trade_date,

        -- Measures
        r.open_price,
        r.high_price,
        r.low_price,
        r.close_price,
        r.volume,
        r.prev_close,
        r.daily_return_pct,
        r.ma_50,
        r.ma_200,
        r.avg_volume_20d,
        r.volume_ratio,
        r.intraday_range_pct,

        -- Derived flags
        r.is_golden_cross,
        r.above_both_mas,

        -- Audit
        r._extracted_at,
        CURRENT_TIMESTAMP                       AS _loaded_at

    FROM returns r
    LEFT JOIN dim_date    dd ON dd.full_date  = r.trade_date
    LEFT JOIN dim_company dc ON dc.ticker     = r.ticker
)

SELECT * FROM joined

{% if is_incremental() %}
-- Incremental filter: only process rows newer than the latest already loaded
WHERE trade_date > (SELECT COALESCE(MAX(trade_date), DATE '1970-01-01') FROM {{ this }})
{% endif %}
