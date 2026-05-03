package com.bigdataspark.config.db;

import lombok.experimental.UtilityClass;

@UtilityClass
public class PostgresDatabaseBeans {

    static final String DATA_SOURCE_PROPERTIES = "_datasource_properties";
    static final String DATA_SOURCE            = "_datasource";
    static final String NAMED_JDBC_TEMPLATE    = "_named_jdbc_template";

    public static final String POSTGRES_PREFIX                = "postgres_etl";
    public static final String POSTGRES_DATASOURCE_PROPERTIES = POSTGRES_PREFIX + DATA_SOURCE_PROPERTIES;
    public static final String POSTGRES_DATASOURCE_BEAN       = POSTGRES_PREFIX + DATA_SOURCE;
    public static final String POSTGRES_NAMED_JDBC_TEMPLATE   = POSTGRES_PREFIX + NAMED_JDBC_TEMPLATE;
}
