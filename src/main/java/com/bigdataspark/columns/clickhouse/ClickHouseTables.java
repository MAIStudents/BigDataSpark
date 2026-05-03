package com.bigdataspark.columns.clickhouse;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ClickHouseTables {

    public static final String DATABASE = "analytics";
    public static final String SALES_BY_PRODUCT = "analytics.sales_by_product";
    public static final String SALES_BY_CUSTOMER = "analytics.sales_by_customer";
    public static final String SALES_BY_TIME = "analytics.sales_by_time";
    public static final String SALES_BY_STORE = "analytics.sales_by_store";
    public static final String SALES_BY_SUPPLIER = "analytics.sales_by_supplier";
    public static final String PRODUCT_QUALITY = "analytics.product_quality";
}
