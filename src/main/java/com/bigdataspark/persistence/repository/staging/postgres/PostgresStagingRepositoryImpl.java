package com.bigdataspark.persistence.repository.staging.postgres;

import com.bigdataspark.config.db.PostgresDatabaseBeans;
import com.bigdataspark.persistence.query.postgres.staging.StagingQuery;
import com.bigdataspark.persistence.repository.staging.StagingRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;

@Repository
@Slf4j
public class PostgresStagingRepositoryImpl implements StagingRepository {

    @Qualifier(PostgresDatabaseBeans.POSTGRES_NAMED_JDBC_TEMPLATE)
    private final NamedParameterJdbcTemplate jdbcTemplate;

    public PostgresStagingRepositoryImpl(
            @Qualifier(PostgresDatabaseBeans.POSTGRES_NAMED_JDBC_TEMPLATE)
            NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void dropStagingTables() {
        log.debug("Dropping staging tables");
        executeScript(StagingQuery.DROP_STAGING_TABLES.query());
    }

    @Override
    public void createStagingTables() {
        log.debug("Creating staging tables");
        executeScript(StagingQuery.CREATE_STAGING_TABLES.query());
    }

    private void executeScript(String sql) {
        DataSource dataSource = jdbcTemplate.getJdbcTemplate().getDataSource();
        try (Connection connection = dataSource.getConnection()) {
            ScriptUtils.executeSqlScript(connection,
                    new org.springframework.core.io.ByteArrayResource(sql.getBytes()));
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to execute SQL script", e);
        }
    }
}
