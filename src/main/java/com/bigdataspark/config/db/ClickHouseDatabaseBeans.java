package com.bigdataspark.config.db;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ClickHouseDatabaseBeans {

    static final String DATA_SOURCE_PROPERTIES = "_datasource_properties";
    static final String DATA_SOURCE            = "_datasource";
    static final String NAMED_JDBC_TEMPLATE    = "_named_jdbc_template";

    public static final String CLICKHOUSE_PREFIX                = "clickhouse_etl";
    public static final String CLICKHOUSE_DATASOURCE_PROPERTIES = CLICKHOUSE_PREFIX + DATA_SOURCE_PROPERTIES;
    public static final String CLICKHOUSE_DATASOURCE_BEAN       = CLICKHOUSE_PREFIX + DATA_SOURCE;
    public static final String CLICKHOUSE_NAMED_JDBC_TEMPLATE   = CLICKHOUSE_PREFIX + NAMED_JDBC_TEMPLATE;
}
