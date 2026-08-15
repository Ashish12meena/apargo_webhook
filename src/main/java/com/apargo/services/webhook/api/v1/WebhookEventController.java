package com.apargo.services.webhook.api.v1;

import com.apargo.services.webhook.api.support.ApiResponse;
import com.apargo.services.webhook.api.v1.dto.BulkReplayRequest;
import com.apargo.services.webhook.api.v1.dto.PageResponse;
import com.apargo.services.webhook.api.v1.dto.ReplayResponse;
import com.apargo.services.webhook.api.v1.dto.WebhookEventDetailResponse;
import com.apargo.services.webhook.api.v1.dto.WebhookEventSummaryResponse;
import com.apargo.services.webhook.application.port.in.QueryEventsUseCase;
import com.apargo.services.webhook.application.port.in.ReplayEventUseCase;
import com.apargo.services.webhook.domain.model.EventSearchCriteria;
import com.apargo.services.webhook.domain.model.EventState;
import com.apargo.services.webhook.domain.model.Lane;
import com.apargo.services.webhook.domain.model.PageResult;
import com.apargo.services.webhook.domain.model.WebhookEvent;
import jakarta.validation.Valid;
import java.time.Instant;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Support endpoints. Internal plane only — guarded by {@code X-Internal-Api-Key}, exactly as the
 * other services on the platform.
 */
@RestController
@RequestMapping("/api/v1/webhook-events")
public class WebhookEventController {

    private static final int MAX_PAGE_SIZE = 200;

    private final QueryEventsUseCase queryEvents;
    private final ReplayEventUseCase replayEvents;

    public WebhookEventController(QueryEventsUseCase queryEvents, ReplayEventUseCase replayEvents) {
        this.queryEvents = queryEvents;
        this.replayEvents = replayEvents;
    }

    /** Paged list, filterable by state, lane, field, phone number, wamid and date range. */
    @GetMapping
    public ResponseEntity<ApiResponse<PageResponse<WebhookEventSummaryResponse>>> list(
            @RequestParam(required = false) EventState state,
            @RequestParam(required = false) Lane lane,
            @RequestParam(required = false) String field,
            @RequestParam(required = false) String providerPhoneNumberId,
            @RequestParam(required = false) String wamid,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {

        EventSearchCriteria criteria = EventSearchCriteria.builder()
                .state(state)
                .lane(lane)
                .field(field)
                .providerPhoneNumberId(providerPhoneNumberId)
                .wamid(wamid)
                .from(from)
                .to(to)
                .page(Math.max(0, page))
                .size(clampSize(size))
                .build();

        PageResult<WebhookEvent> result = queryEvents.search(criteria);
        return ResponseEntity.ok(ApiResponse.success(
                PageResponse.from(result, WebhookEventSummaryResponse::from)));
    }

    /** Full document, including the raw change as Meta sent it. */
    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<WebhookEventDetailResponse>> get(@PathVariable String id) {
        return ResponseEntity.ok(ApiResponse.success(
                WebhookEventDetailResponse.from(queryEvents.getById(id))));
    }

    /** Resets a single event to PENDING so the relay picks it up again. */
    @PostMapping("/{id}/replay")
    public ResponseEntity<ApiResponse<ReplayResponse>> replay(@PathVariable String id) {
        WebhookEvent replayed = replayEvents.replay(id);
        return ResponseEntity.ok(ApiResponse.success(ReplayResponse.single(replayed.id())));
    }

    /** Bulk replay by filter. The date range is mandatory; see {@link BulkReplayRequest}. */
    @PostMapping("/replay")
    public ResponseEntity<ApiResponse<ReplayResponse>> replayAll(
            @Valid @RequestBody BulkReplayRequest request) {

        EventSearchCriteria criteria = EventSearchCriteria.builder()
                .state(request.state())
                .lane(request.lane())
                .field(request.field())
                .providerPhoneNumberId(request.providerPhoneNumberId())
                .from(request.from())
                .to(request.to())
                .build();

        return ResponseEntity.ok(ApiResponse.success(
                ReplayResponse.bulk(replayEvents.replayAll(criteria))));
    }

    private int clampSize(int size) {
        if (size < 1) {
            return 1;
        }
        return Math.min(size, MAX_PAGE_SIZE);
    }
}
