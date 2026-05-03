package com.bigdataspark.config.db;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

import static com.bigdataspark.config.db.PostgresDatabaseBeans.POSTGRES_DATASOURCE_BEAN;
import static com.bigdataspark.config.db.PostgresDatabaseBeans.POSTGRES_NAMED_JDBC_TEMPLATE;

@Configuration
public class PostgresDatabaseConfig {

    @Value("${app.datasource.postgres.url}")
    private String url;

    @Value("${app.datasource.postgres.username}")
    private String username;

    @Value("${app.datasource.postgres.password}")
    private String password;

    @Value("${app.datasource.postgres.driver-class-name:org.postgresql.Driver}")
    private String driverClassName;

    @Value("${app.datasource.postgres.hikari.maximum-pool-size:4}")
    private int maxPoolSize;

    @Value("${app.datasource.postgres.hikari.pool-name:bigdataspark-pg}")
    private String poolName;

    @Primary
    @Bean(POSTGRES_DATASOURCE_BEAN)
    public DataSource postgresDataSource() {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        ds.setDriverClassName(driverClassName);
        ds.setMaximumPoolSize(maxPoolSize);
        ds.setPoolName(poolName);
        return ds;
    }

    @Primary
    @Bean(POSTGRES_NAMED_JDBC_TEMPLATE)
    public NamedParameterJdbcTemplate postgresNamedJdbcTemplate(
            @Qualifier(POSTGRES_DATASOURCE_BEAN) DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean
    public PostgresConnectionProperties postgresConnectionProperties() {
        return new PostgresConnectionProperties(url, username, password);
    }
}
