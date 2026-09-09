package com.apargo.services.webhook.application.service;

import com.apargo.services.webhook.application.port.out.WebhookEventRepositoryPort;
import com.apargo.services.webhook.domain.model.WebhookEvent;
import com.apargo.services.webhook.infrastructure.config.WebhookProperties;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

/**
 * The durable write on the ingest path, in one of two modes.
 *
 * <p>By default it is a straight pass-through to a single bulk insert per request. Turned on, it
 * accumulates events from separate HTTP requests and closes the batch on whichever trigger fires
 * first: records, estimated bytes, or elapsed time.
 *
 * <p>Batching is off by default and that is a position rather than caution. {@code insertAll} is
 * already one bulk write per request, so batching across requests trades latency on the one path
 * where Meta is actually waiting against a saving on the cheapest round trip in the pipeline. The
 * measured bottleneck was the relay, not ingest. Turn this on against a profile, not a hunch.
 *
 * <p>Two properties of the batching mode carry the correctness of the whole thing:
 *
 * <ul>
 *   <li>Every request gets its own completion signal, because the 200 cannot be sent until that
 *       request's own events are durable. A shared signal would let one request's 200 ride on
 *       another request's write.
 *   <li>A flush failure fails <em>every</em> request in the batch, so Meta retries all of them.
 *       Partial success on a bulk write is the trap here.
 * </ul>
 *
 * <p>The buffer is guarded by a {@link ReentrantLock} rather than {@code synchronized}. Requests are
 * served on virtual threads, and a virtual thread blocking inside a monitor pins its carrier thread;
 * blocking on a {@code ReentrantLock} does not. There is no {@code synchronized} anywhere in this
 * service, and this is the class that would most easily have introduced one.
 */
@Slf4j
@Component
public class IngestWriter {

    private final WebhookEventRepositoryPort repository;
    private final TaskScheduler taskScheduler;

    private final boolean batchingEnabled;
    private final int maxRecords;
    private final long maxBytes;
    private final Duration maxWait;

    private final ReentrantLock lock = new ReentrantLock();
    private final List<PendingWrite> buffer = new ArrayList<>();
    private long bufferedRecords;
    private long bufferedBytes;
    private ScheduledFuture<?> timerFlush;

    public IngestWriter(
            WebhookEventRepositoryPort repository,
            TaskScheduler taskScheduler,
            WebhookProperties properties) {
        this.repository = repository;
        this.taskScheduler = taskScheduler;

        WebhookProperties.Ingest.Batch batch = properties.ingest().batch();
        this.batchingEnabled = batch.enabled();
        this.maxRecords = batch.maxRecords();
        this.maxBytes = batch.maxBytes().toBytes();
        this.maxWait = batch.maxWait();

        if (batchingEnabled) {
            log.info("Ingest batching ENABLED: closing at {} records, {} bytes or {} — whichever "
                            + "fires first. Every Meta response is delayed by up to the last of those.",
                    maxRecords, maxBytes, maxWait);
        }
    }

    /**
     * Durably writes a request's events and returns them carrying their assigned ids.
     *
     * @throws RuntimeException when the write fails, so the caller answers 500 and Meta retries
     */
    public List<WebhookEvent> write(List<WebhookEvent> events) {
        if (events == null || events.isEmpty()) {
            return List.of();
        }
        if (!batchingEnabled) {
            return repository.insertAll(events);
        }
        return writeBatched(events);
    }

    // -----------------------------------------------------------------------
    // Batched mode
    // -----------------------------------------------------------------------

    private List<WebhookEvent> writeBatched(List<WebhookEvent> events) {
        PendingWrite pending = new PendingWrite(events, new CompletableFuture<>());
        List<PendingWrite> readyToFlush = null;

        lock.lock();
        try {
            buffer.add(pending);
            bufferedRecords += events.size();
            bufferedBytes += BsonSizeEstimator.estimate(events);

            if (buffer.size() == 1) {
                scheduleTimerFlush();
            }
            if (bufferedRecords >= maxRecords || bufferedBytes >= maxBytes) {
                readyToFlush = takeBuffer();
            }
        } finally {
            lock.unlock();
        }

        // Flush outside the lock: the Mongo round trip must not hold up the next request's append.
        if (readyToFlush != null) {
            flush(readyToFlush);
        }

        return awaitWrite(pending);
    }

    private List<WebhookEvent> awaitWrite(PendingWrite pending) {
        try {
            // Blocking here is correct and cheap: the caller is a virtual thread, and the 200 to
            // Meta must not be sent before this request's own events are durable.
            return pending.future().join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException("Batched ingest write failed", cause == null ? e : cause);
        }
    }

    /** Caller must hold the lock. */
    private void scheduleTimerFlush() {
        cancelTimerFlush();
        try {
            // Wall clock, not the injected Clock: this schedules real work on a real executor,
            // and a fixed Clock in a test would mean a flush that never fires.
            timerFlush = taskScheduler.schedule(
                    this::flushOnTimer, java.time.Instant.now().plus(maxWait));
        } catch (RuntimeException e) {
            // If the timer cannot be scheduled, a light-traffic batch would sit until the record
            // trigger eventually fires. Flush immediately instead of holding a request open.
            log.warn("Could not schedule the ingest batch timer; flushing immediately: {}",
                    e.getMessage());
            List<PendingWrite> immediate = takeBuffer();
            if (!immediate.isEmpty()) {
                flush(immediate);
            }
        }
    }

    /** Caller must hold the lock. */
    private void cancelTimerFlush() {
        if (timerFlush != null) {
            timerFlush.cancel(false);
            timerFlush = null;
        }
    }

    /** Caller must hold the lock. Empties the buffer and resets its counters. */
    private List<PendingWrite> takeBuffer() {
        if (buffer.isEmpty()) {
            return List.of();
        }
        List<PendingWrite> taken = List.copyOf(buffer);
        buffer.clear();
        bufferedRecords = 0;
        bufferedBytes = 0;
        cancelTimerFlush();
        return taken;
    }

    private void flushOnTimer() {
        List<PendingWrite> taken;
        lock.lock();
        try {
            taken = takeBuffer();
        } finally {
            lock.unlock();
        }
        if (!taken.isEmpty()) {
            flush(taken);
        }
    }

    /**
     * Writes one accumulated batch and hands each request back its own slice.
     *
     * <p>The insert preserves order, so each request's events are the contiguous run at its offset.
     */
    private void flush(List<PendingWrite> batch) {
        List<WebhookEvent> combined = new ArrayList<>();
        for (PendingWrite pending : batch) {
            combined.addAll(pending.events());
        }

        try {
            List<WebhookEvent> stored = repository.insertAll(combined);

            if (stored.size() != combined.size()) {
                // Partial success on a bulk write is the trap: some requests would get a 200 for
                // events that were never written, and Meta never sends them again.
                throw new IllegalStateException("Bulk insert acknowledged " + stored.size()
                        + " of " + combined.size() + " documents");
            }

            int offset = 0;
            for (PendingWrite pending : batch) {
                int size = pending.events().size();
                pending.future().complete(List.copyOf(stored.subList(offset, offset + size)));
                offset += size;
            }
            log.debug("Ingest batch flushed: {} event(s) from {} request(s) in one write",
                    combined.size(), batch.size());

        } catch (RuntimeException e) {
            // Fail every request in the batch. Anything else means answering 200 to a request whose
            // events are not durable, and Meta offers no replay.
            log.error("Ingest batch of {} event(s) from {} request(s) failed; all of them will be "
                    + "answered 500 so Meta retries", combined.size(), batch.size(), e);
            for (PendingWrite pending : batch) {
                pending.future().completeExceptionally(e);
            }
        }
    }

    /** Nothing accumulated may be left holding a request open when the context closes. */
    @PreDestroy
    void drainOnShutdown() {
        List<PendingWrite> taken;
        lock.lock();
        try {
            taken = takeBuffer();
        } finally {
            lock.unlock();
        }
        if (!taken.isEmpty()) {
            log.info("Flushing {} buffered ingest request(s) before shutdown", taken.size());
            flush(taken);
        }
    }

    /**
     * One request's events and the signal that releases its response.
     *
     * @param events this request's events, in order
     * @param future completed with the stored events, or failed so the request answers 500
     */
    private record PendingWrite(
            List<WebhookEvent> events, CompletableFuture<List<WebhookEvent>> future) {

        private PendingWrite {
            events = Collections.unmodifiableList(new ArrayList<>(events));
        }
    }
}
