MERGE INTO snowflake.dim_country AS t
USING public.etl_stg_country AS s
ON t.country_name = s.country_name
WHEN NOT MATCHED THEN
    INSERT (country_name) VALUES (s.country_name);
