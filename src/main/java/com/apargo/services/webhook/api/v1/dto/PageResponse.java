package com.apargo.services.webhook.api.v1.dto;

import com.apargo.services.webhook.domain.model.PageResult;
import java.util.List;
import java.util.function.Function;

/** Page shape shared by every paged endpoint. */
public record PageResponse<T>(
        List<T> content,
        long totalElements,
        int totalPages,
        int page,
        int size) {

    public static <D, T> PageResponse<T> from(PageResult<D> result, Function<D, T> mapper) {
        return new PageResponse<>(
                result.content().stream().map(mapper).toList(),
                result.totalElements(),
                result.totalPages(),
                result.page(),
                result.size());
    }
}
