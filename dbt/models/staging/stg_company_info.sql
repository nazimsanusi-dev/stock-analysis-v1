-- stg_company_info: cleaned view over the raw company-metadata Parquet lake.
-- One row per ticker (most recent extraction wins via ROW_NUMBER).

{{ config(materialized='view') }}

{% set lake_path = var('data_lake_path', '/opt/airflow/data/lake') %}

WITH source AS (
    SELECT *
    FROM read_parquet(
        '{{ lake_path }}/raw/company_info/**/*.parquet',
        hive_partitioning = true,
        union_by_name     = true
    )
),

renamed AS (
    SELECT
        UPPER(TRIM(ticker))        AS ticker,
        TRIM(company_name)         AS company_name,
        TRIM(sector)               AS sector,
        TRIM(industry)             AS industry,
        TRIM(country)              AS country,
        TRIM(exchange)             AS exchange,
        COALESCE(TRIM(currency), 'USD') AS currency,
        TRY_CAST(market_cap AS BIGINT)  AS market_cap,
        TRY_CAST(employees  AS INTEGER) AS employees,
        TRIM(website)              AS website,
        LEFT(TRIM(description), 500) AS description,
        TRY_CAST(_extracted_at AS TIMESTAMP) AS _extracted_at,
        CURRENT_TIMESTAMP          AS _loaded_at
    FROM source
    WHERE ticker IS NOT NULL
),

-- Keep only the most recently extracted record per ticker
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker
            ORDER BY _extracted_at DESC NULLS LAST
        ) AS _row_num
    FROM renamed
)

SELECT
    ticker,
    company_name,
    sector,
    industry,
    country,
    exchange,
    currency,
    market_cap,
    employees,
    website,
    description,
    _extracted_at,
    _loaded_at
FROM deduped
WHERE _row_num = 1
