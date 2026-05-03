package com.bigdataspark.persistence.query.postgres.staging;

import com.bigdataspark.persistence.query.QueryHolder;

public enum StagingQuery implements QueryHolder {

    DROP_STAGING_TABLES("/sql/postgres/staging/drop_staging_tables.sql"),
    CREATE_STAGING_TABLES("/sql/postgres/staging/create_staging_tables.sql");

    private final String query;

    StagingQuery(String path) {
        this.query = getQueryValue(path);
    }

    @Override
    public String query() {
        return query;
    }
}
