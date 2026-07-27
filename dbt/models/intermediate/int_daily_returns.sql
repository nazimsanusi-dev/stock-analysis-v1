-- int_daily_returns: enriches price rows with return metrics and
-- rolling moving-average signals used by the marts layer.
-- Materialised as a view so no storage cost; marts re-use via ref().

{{ config(materialized='view') }}

WITH prices AS (
    SELECT * FROM {{ ref('stg_raw_prices') }}
),

with_returns AS (
    SELECT
        ticker,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        _extracted_at,
        _loaded_at,

        -- Daily return vs previous trading day
        LAG(close_price) OVER (
            PARTITION BY ticker ORDER BY trade_date
        )                                                   AS prev_close,

        ROUND(
            (close_price - LAG(close_price) OVER (
                PARTITION BY ticker ORDER BY trade_date
            )) / NULLIF(LAG(close_price) OVER (
                PARTITION BY ticker ORDER BY trade_date
            ), 0) * 100,
        4)                                                  AS daily_return_pct,

        -- Rolling moving averages
        ROUND(AVG(close_price) OVER (
            PARTITION BY ticker ORDER BY trade_date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ), 4)                                               AS ma_50,

        ROUND(AVG(close_price) OVER (
            PARTITION BY ticker ORDER BY trade_date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ), 4)                                               AS ma_200,

        -- 20-day average daily volume (for volume-surge detection)
        ROUND(AVG(volume) OVER (
            PARTITION BY ticker ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 0)                                               AS avg_volume_20d,

        -- Intraday range
        ROUND((high_price - low_price) / NULLIF(low_price, 0) * 100, 4)
                                                            AS intraday_range_pct

    FROM prices
)

SELECT
    ticker,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    prev_close,
    daily_return_pct,
    ma_50,
    ma_200,
    avg_volume_20d,
    ROUND(volume / NULLIF(avg_volume_20d, 0), 2)        AS volume_ratio,
    intraday_range_pct,
    -- Golden-cross flag: MA50 crossed above MA200 today
    CASE
        WHEN ma_50 > ma_200
         AND LAG(ma_50) OVER (PARTITION BY ticker ORDER BY trade_date)
             <= LAG(ma_200) OVER (PARTITION BY ticker ORDER BY trade_date)
        THEN TRUE
        ELSE FALSE
    END                                                     AS is_golden_cross,
    -- Price above both MAs
    (close_price > ma_50 AND close_price > ma_200)          AS above_both_mas,
    _extracted_at,
    _loaded_at
FROM with_returns
