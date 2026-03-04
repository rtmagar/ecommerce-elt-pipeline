SELECT
    product_category_name,
    product_category_name_english
FROM {{ source('olist', 'raw_product_category_name_translation') }}