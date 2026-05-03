package com.bigdataspark.dto;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "ETL завершён без ошибок")
public record JobOkResponse(
        @Schema(description = "Сообщение о статусе")
        String status
) {
}
