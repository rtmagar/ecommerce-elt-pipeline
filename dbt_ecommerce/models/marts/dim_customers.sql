{{ config(materialized='table') }}

SELECT DISTINCT
    customer_id,
    customer_unique_id,
    zip_code,
    city,
    state
FROM {{ ref('stg_customers') }}