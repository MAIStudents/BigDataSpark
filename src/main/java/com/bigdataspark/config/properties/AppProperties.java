package com.bigdataspark.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public record AppProperties(SparkProperties spark) {

    public record SparkProperties(String master, String hadoopHomeDir) {
    }
}
