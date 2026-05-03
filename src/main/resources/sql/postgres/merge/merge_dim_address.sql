MERGE INTO snowflake.dim_address AS t
USING public.etl_stg_address AS s
ON t.city_id = s.city_id
   AND t.postal_code IS NOT DISTINCT FROM s.postal_code
WHEN NOT MATCHED THEN
    INSERT (postal_code, city_id) VALUES (s.postal_code, s.city_id);
