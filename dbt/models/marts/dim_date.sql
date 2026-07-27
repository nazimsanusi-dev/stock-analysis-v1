-- dim_date: pre-built date dimension spanning 2018-01-01 → 2035-12-31.
-- Generated entirely in SQL using DuckDB's generate_series function.
-- Re-materialised as a table so no scan on each query.

{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT
        CAST(
            UNNEST(
                generate_series(DATE '2018-01-01', DATE '2035-12-31', INTERVAL 1 DAY)
            )
        AS DATE) AS full_date
),

enriched AS (
    SELECT
        full_date,
        CAST(strftime(full_date, '%Y%m%d') AS INTEGER)  AS date_sk,
        EXTRACT('year'    FROM full_date)::INTEGER       AS year,
        EXTRACT('quarter' FROM full_date)::INTEGER       AS quarter,
        EXTRACT('month'   FROM full_date)::INTEGER       AS month,
        strftime(full_date, '%B')                        AS month_name,
        strftime(full_date, '%b')                        AS month_abbr,
        EXTRACT('week'    FROM full_date)::INTEGER       AS week_of_year,
        EXTRACT('day'     FROM full_date)::INTEGER       AS day_of_month,
        -- DuckDB DOW: 0=Sunday … 6=Saturday
        EXTRACT('dow'     FROM full_date)::INTEGER       AS day_of_week,
        strftime(full_date, '%A')                        AS day_name,
        strftime(full_date, '%a')                        AS day_abbr,
        EXTRACT('dow' FROM full_date) IN (0, 6)         AS is_weekend,
        EXTRACT('dow' FROM full_date) NOT IN (0, 6)     AS is_weekday,
        -- Approximate NYSE trading day: weekday, not a fixed US holiday
        (
            EXTRACT('dow' FROM full_date) NOT IN (0, 6)
            AND NOT (EXTRACT('month' FROM full_date) = 1  AND EXTRACT('day' FROM full_date) = 1)   -- New Year
            AND NOT (EXTRACT('month' FROM full_date) = 7  AND EXTRACT('day' FROM full_date) = 4)   -- Independence Day
            AND NOT (EXTRACT('month' FROM full_date) = 12 AND EXTRACT('day' FROM full_date) = 25)  -- Christmas
        )                                                AS is_approx_trading_day,
        CASE
            WHEN EXTRACT('month' FROM full_date) IN (3,4,5)   THEN 'Spring'
            WHEN EXTRACT('month' FROM full_date) IN (6,7,8)   THEN 'Summer'
            WHEN EXTRACT('month' FROM full_date) IN (9,10,11) THEN 'Fall'
            ELSE 'Winter'
        END                                              AS season,
        -- Fiscal quarters (assume Jan fiscal year start)
        'Q' || EXTRACT('quarter' FROM full_date)::VARCHAR AS fiscal_quarter_label,
        EXTRACT('year' FROM full_date)::VARCHAR || '-Q'
            || EXTRACT('quarter' FROM full_date)::VARCHAR AS year_quarter
    FROM date_spine
)

SELECT * FROM enriched
ORDER BY full_date
