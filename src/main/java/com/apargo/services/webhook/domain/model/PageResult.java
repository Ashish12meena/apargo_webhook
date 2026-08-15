package com.apargo.services.webhook.domain.model;

import java.util.List;

/**
 * Transport-agnostic page of results, so the application layer never depends on Spring Data's
 * {@code Page} in its public contracts.
 */
public record PageResult<T>(List<T> content, long totalElements, int page, int size) {

    public PageResult {
        content = content == null ? List.of() : List.copyOf(content);
    }

    public int totalPages() {
        return size <= 0 ? 0 : (int) Math.ceil((double) totalElements / size);
    }

    public <R> PageResult<R> map(java.util.function.Function<T, R> mapper) {
        return new PageResult<>(content.stream().map(mapper).toList(), totalElements, page, size);
    }
}
