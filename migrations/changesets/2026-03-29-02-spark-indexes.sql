-- liquibase formatted sql
-- changeset admin:2026-03-29-02-spark-indexes labels:spark_indexes

CREATE INDEX idx_fact_sales_customer_id ON snowflake.fact_sales (customer_id);
CREATE INDEX idx_fact_sales_pet_id ON snowflake.fact_sales (pet_id);
CREATE INDEX idx_fact_sales_seller_id ON snowflake.fact_sales (seller_id);
CREATE INDEX idx_fact_sales_store_id ON snowflake.fact_sales (store_id);
CREATE INDEX idx_fact_sales_product_id ON snowflake.fact_sales (product_id);
CREATE INDEX idx_fact_sales_sale_date_product_id ON snowflake.fact_sales (sale_date, product_id);
CREATE INDEX idx_fact_sales_total_price ON snowflake.fact_sales (total_price);
CREATE INDEX idx_dim_product_product_id_rating ON snowflake.dim_product (product_id, rating);

--rollback DROP INDEX IF EXISTS snowflake.idx_dim_product_product_id_rating;
--rollback DROP INDEX IF EXISTS snowflake.idx_fact_sales_total_price;
--rollback DROP INDEX IF EXISTS snowflake.idx_fact_sales_sale_date_product_id;
--rollback DROP INDEX IF EXISTS snowflake.idx_fact_sales_product_id;
--rollback DROP INDEX IF EXISTS snowflake.idx_fact_sales_store_id;
--rollback DROP INDEX IF EXISTS snowflake.idx_fact_sales_seller_id;
--rollback DROP INDEX IF EXISTS snowflake.idx_fact_sales_pet_id;
--rollback DROP INDEX IF EXISTS snowflake.idx_fact_sales_customer_id;