package com.bigdataspark.persistence.query.postgres.fact;

import com.bigdataspark.persistence.query.QueryHolder;

public enum FactSalesQuery implements QueryHolder {

    TRUNCATE_FACT_SALES("/sql/postgres/fact/truncate_fact_sales.sql"),
    INSERT_FACT_SALES_FROM_STAGING("/sql/postgres/fact/insert_fact_sales_from_staging.sql");

    private final String query;

    FactSalesQuery(String path) {
        this.query = getQueryValue(path);
    }

    @Override
    public String query() {
        return query;
    }
}
