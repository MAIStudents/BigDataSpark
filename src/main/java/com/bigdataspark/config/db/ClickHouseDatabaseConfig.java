package com.bigdataspark.config.db;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

import static com.bigdataspark.config.db.ClickHouseDatabaseBeans.CLICKHOUSE_DATASOURCE_BEAN;
import static com.bigdataspark.config.db.ClickHouseDatabaseBeans.CLICKHOUSE_NAMED_JDBC_TEMPLATE;

@Configuration
public class ClickHouseDatabaseConfig {

    @Value("${app.datasource.clickhouse.url}")
    private String url;

    @Value("${app.datasource.clickhouse.username}")
    private String username;

    @Value("${app.datasource.clickhouse.password}")
    private String password;

    @Value("${app.datasource.clickhouse.driver-class-name:com.clickhouse.jdbc.ClickHouseDriver}")
    private String driverClassName;

    @Value("${app.datasource.clickhouse.hikari.maximum-pool-size:4}")
    private int maxPoolSize;

    @Value("${app.datasource.clickhouse.hikari.pool-name:bigdataspark-ch}")
    private String poolName;

    @Bean(CLICKHOUSE_DATASOURCE_BEAN)
    public DataSource clickHouseDataSource() {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        ds.setDriverClassName(driverClassName);
        ds.setMaximumPoolSize(maxPoolSize);
        ds.setPoolName(poolName);
        return ds;
    }

    @Bean(CLICKHOUSE_NAMED_JDBC_TEMPLATE)
    public NamedParameterJdbcTemplate clickHouseNamedJdbcTemplate(
            @Qualifier(CLICKHOUSE_DATASOURCE_BEAN) DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean
    public ClickHouseConnectionProperties clickHouseConnectionProperties() {
        return new ClickHouseConnectionProperties(url, username, password, driverClassName);
    }
}
