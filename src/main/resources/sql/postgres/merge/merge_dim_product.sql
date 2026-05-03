MERGE INTO snowflake.dim_product AS t
USING public.etl_stg_product AS s
ON t.product_name = s.product_name
   AND t.supplier_id = s.supplier_id
   AND t.category IS NOT DISTINCT FROM s.category
   AND t.brand IS NOT DISTINCT FROM s.brand
WHEN NOT MATCHED THEN
    INSERT (product_name, category, price, weight, color, size, brand, material, description,
            rating, reviews, release_date, expiry_date, supplier_id)
    VALUES (s.product_name, s.category, s.price, s.weight, s.color, s.size, s.brand, s.material,
            LEFT(s.description, 1024), s.rating, s.reviews,
            CASE WHEN s.release_date IS NULL OR TRIM(s.release_date) = '' THEN NULL
                 ELSE LEFT(TRIM(s.release_date), 10)::date END,
            CASE WHEN s.expiry_date IS NULL OR TRIM(s.expiry_date) = '' THEN NULL
                 ELSE LEFT(TRIM(s.expiry_date), 10)::date END,
            s.supplier_id);
