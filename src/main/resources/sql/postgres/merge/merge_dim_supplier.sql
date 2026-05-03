MERGE INTO snowflake.dim_supplier AS t
USING public.etl_stg_supplier AS s
ON t.supplier_name = s.supplier_name
   AND t.email IS NOT DISTINCT FROM s.email
WHEN NOT MATCHED THEN
    INSERT (supplier_name, contact, email, phone, address_id)
    VALUES (s.supplier_name, s.contact, s.email, s.phone, s.address_id);
