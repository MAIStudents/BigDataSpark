package com.bigdataspark.config.db;

import java.util.Properties;

public record ClickHouseConnectionProperties(
        String url,
        String username,
        String password,
        String driverClassName
) {

    public Properties sparkJdbcProperties() {
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        props.setProperty("driver", driverClassName);
        props.setProperty("compress", "false");
        props.setProperty("jdbcCompliant", "false");
        return props;
    }
}
