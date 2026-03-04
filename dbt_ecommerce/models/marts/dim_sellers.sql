{{ config(materialized='table') }}

SELECT DISTINCT
    seller_id,
    zip_code,
    city,
    state
FROM {{ ref('stg_sellers') }}