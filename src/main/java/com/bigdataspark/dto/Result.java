package com.bigdataspark.dto;

import lombok.Getter;

@Getter
public class Result<T> {

    private final T payload;
    private final int resultCode;
    private final String message;

    private Result(T payload, int resultCode, String message) {
        this.payload = payload;
        this.resultCode = resultCode;
        this.message = message;
    }

    public static <T> Result<T> successResult(T payload) {
        return new Result<>(payload, 0, "OK");
    }

    public static Result<Void> emptyResult() {
        return new Result<>(null, 0, "OK");
    }

    public static Result<Void> errorResult(String message) {
        return new Result<>(null, -1, message);
    }
}
