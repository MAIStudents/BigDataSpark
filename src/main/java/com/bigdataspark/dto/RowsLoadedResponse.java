package com.bigdataspark.dto;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Результат загрузки CSV в public.mock_data")
public record RowsLoadedResponse(
        @Schema(description = "Число вставленных строк")
        long rowsLoaded
) {
}
