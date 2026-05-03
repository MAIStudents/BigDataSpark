package com.bigdataspark.persistence.repository.schema.clickhouse;

import com.bigdataspark.config.db.ClickHouseDatabaseBeans;
import com.bigdataspark.persistence.query.clickhouse.ClickHouseDdlQuery;
import com.bigdataspark.persistence.repository.schema.ClickHouseSchemaRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@Slf4j
public class ClickHouseSchemaRepositoryImpl implements ClickHouseSchemaRepository {

    @Qualifier(ClickHouseDatabaseBeans.CLICKHOUSE_NAMED_JDBC_TEMPLATE)
    private final NamedParameterJdbcTemplate jdbcTemplate;

    public ClickHouseSchemaRepositoryImpl(
            @Qualifier(ClickHouseDatabaseBeans.CLICKHOUSE_NAMED_JDBC_TEMPLATE)
            NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void recreateMartTables() {
        log.info("Recreating ClickHouse mart tables");
        String script = ClickHouseDdlQuery.RECREATE_MART_TABLES.query();
        for (String statement : script.split(";")) {
            String sql = statement.trim();
            if (!sql.isEmpty()) {
                log.debug("ClickHouse DDL: {}", sql.substring(0, Math.min(80, sql.length())));
                jdbcTemplate.getJdbcOperations().execute(sql);
            }
        }
    }
}
