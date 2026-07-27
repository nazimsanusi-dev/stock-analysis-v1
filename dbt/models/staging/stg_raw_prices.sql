-- stg_raw_prices: cleaned view over the raw Parquet price lake.
-- Renames columns, enforces types, removes obviously invalid rows.
-- Uses read_parquet() with hive_partitioning so DuckDB can push down
-- date-range filters when downstream models use WHERE clauses.

{{ config(materialized='view') }}

{% set lake_path = var('data_lake_path', '/opt/airflow/data/lake') %}

WITH source AS (
    SELECT *
    FROM read_parquet(
        '{{ lake_path }}/raw/prices/**/*.parquet',
        hive_partitioning = true,
        union_by_name     = true
    )
),

renamed AS (
    SELECT
        UPPER(TRIM(ticker))                           AS ticker,
        CAST("Date"   AS DATE)                        AS trade_date,
        ROUND(CAST("Open"   AS DOUBLE), 6)            AS open_price,
        ROUND(CAST("High"   AS DOUBLE), 6)            AS high_price,
        ROUND(CAST("Low"    AS DOUBLE), 6)            AS low_price,
        ROUND(CAST("Close"  AS DOUBLE), 6)            AS close_price,
        CAST("Volume" AS BIGINT)                      AS volume,
        TRY_CAST(_extracted_at AS TIMESTAMP)          AS _extracted_at,
        CURRENT_TIMESTAMP                             AS _loaded_at
    FROM source
),

validated AS (
    SELECT *
    FROM renamed
    WHERE ticker      IS NOT NULL
      AND trade_date  IS NOT NULL
      AND close_price IS NOT NULL
      AND close_price > 0
      AND volume      > 0
)

SELECT * FROM validated
