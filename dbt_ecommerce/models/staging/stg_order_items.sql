SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    (price + freight_value) AS total_item_value
FROM {{ source('olist', 'raw_order_items') }}