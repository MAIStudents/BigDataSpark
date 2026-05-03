package com.bigdataspark.persistence.repository.fact.postgres;

import com.bigdataspark.config.db.PostgresDatabaseBeans;
import com.bigdataspark.persistence.query.postgres.fact.FactSalesQuery;
import com.bigdataspark.persistence.repository.fact.FactSalesRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@Slf4j
public class PostgresFactSalesRepositoryImpl implements FactSalesRepository {

    @Qualifier(PostgresDatabaseBeans.POSTGRES_NAMED_JDBC_TEMPLATE)
    private final NamedParameterJdbcTemplate jdbcTemplate;

    public PostgresFactSalesRepositoryImpl(
            @Qualifier(PostgresDatabaseBeans.POSTGRES_NAMED_JDBC_TEMPLATE)
            NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    @Transactional
    public void truncateAndInsertFromStaging() {
        log.info("Truncating fact_sales and inserting from staging");
        jdbcTemplate.getJdbcOperations().execute(FactSalesQuery.TRUNCATE_FACT_SALES.query());
        jdbcTemplate.getJdbcOperations().execute(FactSalesQuery.INSERT_FACT_SALES_FROM_STAGING.query());
    }
}
