package com.bigdataspark.persistence.query;

import com.bigdataspark.exception.SqlQueryException;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public interface QueryHolder {

    String query();

    default String getQueryValue(String path) {
        try {
            return IOUtils.resourceToString(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new SqlQueryException("Cannot load SQL resource: " + path, e);
        }
    }
}
