package com.bigdataspark.config.db;

import java.util.Properties;

public record PostgresConnectionProperties(String url, String username, String password) {

    public Properties sparkConnectionProperties() {
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        return props;
    }
}
