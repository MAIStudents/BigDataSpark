MERGE INTO snowflake.dim_customer AS t
USING public.etl_stg_customer AS s
ON t.email = s.email
WHEN NOT MATCHED THEN
    INSERT (first_name, last_name, age, email, address_id)
    VALUES (s.first_name, s.last_name, s.age, s.email, s.address_id);
