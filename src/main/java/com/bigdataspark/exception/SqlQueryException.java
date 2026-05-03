package com.bigdataspark.exception;

public class SqlQueryException extends RuntimeException {

    public SqlQueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
