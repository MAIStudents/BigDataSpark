MERGE INTO snowflake.dim_store AS t
USING public.etl_stg_store AS s
ON t.store_name = s.store_name
   AND t.email IS NOT DISTINCT FROM s.email
   AND t.phone IS NOT DISTINCT FROM s.phone
WHEN NOT MATCHED THEN
    INSERT (store_name, address_id, phone, email)
    VALUES (s.store_name, s.address_id, s.phone, s.email);
