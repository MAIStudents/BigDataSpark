package com.bigdataspark.columns.postgres.snowflake;

import lombok.experimental.UtilityClass;

@UtilityClass
public class SnowflakeTables {

    public static final String SCHEMA = "snowflake";
    public static final String DIM_COUNTRY = "snowflake.dim_country";
    public static final String DIM_CITY = "snowflake.dim_city";
    public static final String DIM_ADDRESS = "snowflake.dim_address";
    public static final String DIM_CUSTOMER = "snowflake.dim_customer";
    public static final String DIM_PET = "snowflake.dim_pet";
    public static final String DIM_SELLER = "snowflake.dim_seller";
    public static final String DIM_STORE = "snowflake.dim_store";
    public static final String DIM_SUPPLIER = "snowflake.dim_supplier";
    public static final String DIM_PRODUCT = "snowflake.dim_product";
    public static final String FACT_SALES = "snowflake.fact_sales";
}
