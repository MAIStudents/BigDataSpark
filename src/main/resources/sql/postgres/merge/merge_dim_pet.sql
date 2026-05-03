MERGE INTO snowflake.dim_pet AS t
USING public.etl_stg_pet AS s
ON t.pet_name = s.pet_name
   AND t.pet_type IS NOT DISTINCT FROM s.pet_type
   AND t.pet_breed IS NOT DISTINCT FROM s.pet_breed
   AND t.pet_category IS NOT DISTINCT FROM s.pet_category
WHEN NOT MATCHED THEN
    INSERT (pet_name, pet_type, pet_breed, pet_category)
    VALUES (s.pet_name, s.pet_type, s.pet_breed, s.pet_category);
