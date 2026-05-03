MERGE INTO snowflake.dim_seller AS t
USING public.etl_stg_seller AS s
ON t.email = s.email
WHEN NOT MATCHED THEN
    INSERT (first_name, last_name, email, address_id)
    VALUES (s.first_name, s.last_name, s.email, s.address_id);
