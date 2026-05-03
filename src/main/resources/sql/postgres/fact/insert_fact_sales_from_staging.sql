INSERT INTO snowflake.fact_sales (
    customer_id, pet_id, seller_id, store_id, product_id, sale_date, quantity, total_price
)
SELECT customer_id,
       pet_id,
       seller_id,
       store_id,
       product_id,
       sale_date::date AS sale_date,
       quantity,
       total_price
FROM public.etl_stg_fact;
