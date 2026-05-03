package com.bigdataspark.persistence.query.clickhouse;

import com.bigdataspark.persistence.query.QueryHolder;

public enum ClickHouseDdlQuery implements QueryHolder {

    RECREATE_MART_TABLES("/sql/clickhouse/clickhouse_create_marts.sql");

    private final String query;

    ClickHouseDdlQuery(String path) {
        this.query = getQueryValue(path);
    }

    @Override
    public String query() {
        return query;
    }
}
