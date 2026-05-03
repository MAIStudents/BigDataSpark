MERGE INTO snowflake.dim_city AS t
USING (
    SELECT s.city_name, c.country_id
    FROM public.etl_stg_city s
    INNER JOIN snowflake.dim_country c ON c.country_name = s.country_name
) AS s
ON t.city_name = s.city_name AND t.country_id = s.country_id
WHEN NOT MATCHED THEN
    INSERT (city_name, country_id) VALUES (s.city_name, s.country_id);
