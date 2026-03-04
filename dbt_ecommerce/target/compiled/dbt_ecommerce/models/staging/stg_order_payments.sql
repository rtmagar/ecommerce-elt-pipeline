SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM "analytics_warehouse"."public"."raw_order_payments"