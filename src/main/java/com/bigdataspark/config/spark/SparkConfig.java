package com.bigdataspark.config.spark;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;

@Configuration
public class SparkConfig {

    @Bean(destroyMethod = "stop")
    public SparkSession sparkSession(
            @Value("${app.spark.master:local[*]}") String master,
            @Value("${app.spark.hadoop-home-dir:}") String hadoopHomeOverride) {
        String home = (hadoopHomeOverride != null && !hadoopHomeOverride.isBlank())
                ? hadoopHomeOverride
                : System.getenv().getOrDefault("HADOOP_HOME",
                System.getProperty("java.io.tmpdir") + "/hadoop-spark");
        new File(home, "bin").mkdirs();
        System.setProperty("hadoop.home.dir", home);
        return SparkSession.builder()
                .appName("bigdataspark-etl")
                .master(master)
                .getOrCreate();
    }
}
