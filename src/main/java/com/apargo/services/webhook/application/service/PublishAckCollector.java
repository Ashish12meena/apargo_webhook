package com.apargo.services.webhook.application.service;

import com.apargo.services.webhook.application.port.out.WebhookEventRepositoryPort;
import com.apargo.services.webhook.infrastructure.config.WebhookProperties;
import com.apargo.services.webhook.infrastructure.metrics.WebhookMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Collects broker acknowledgements and marks them PUBLISHED in bulk, away from the producer's sender
 * thread.
 *
 * <p>This class exists because of one measurement. The relay used to call {@code markPublished}
 * directly inside the producer callback, and those callbacks all run on the {@code KafkaProducer}'s
 * single sender thread — the one thread in the JVM where nothing can proceed in parallel. Every
 * confirmation queued behind the previous one's Mongo round trip, which pinned the whole service at
 * a median 32ms per event: 30 events/sec, with almost no variance, which is the signature of a
 * serialized fixed cost rather than contention.
 *
 * <p>The fix is not a faster write. It is no write at all on that thread. {@link #acknowledge} does
 * a queue append and returns; a dedicated flusher thread does the database work. Note that putting a
 * <em>bulk</em> write in the callback instead of a single one would have rebuilt exactly the same
 * bottleneck with a bigger unit — the position of the I/O was the problem, not its size.
 *
 * <p>The flusher is a single dedicated thread rather than a slot in the shared scheduler pool,
 * because a stalled Mongo write here must not also stall the lease reclaim sweep or the relay-lag
 * gauge.
 */
@Slf4j
@Component
public class PublishAckCollector {

    private final WebhookEventRepositoryPort repository;
    private final WebhookMetrics metrics;
    private final Clock clock;

    private final int flushSize;
    private final long flushIntervalMillis;

    private final Queue<String> acknowledged = new ConcurrentLinkedQueue<>();
    private final AtomicInteger depth = new AtomicInteger();
    private final AtomicBoolean flushing = new AtomicBoolean();

    private ScheduledExecutorService flusher;

    public PublishAckCollector(
            WebhookEventRepositoryPort repository,
            WebhookMetrics metrics,
            WebhookProperties properties,
            Clock clock) {
        this.repository = repository;
        this.metrics = metrics;
        this.clock = clock;
        this.flushSize = properties.publisher().ackFlushSize();
        this.flushIntervalMillis = properties.publisher().ackFlushInterval().toMillis();
    }

    @PostConstruct
    void start() {
        flusher = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "webhook-ack-flusher");
            thread.setDaemon(true);
            return thread;
        });
        flusher.scheduleWithFixedDelay(
                this::flushQuietly, flushIntervalMillis, flushIntervalMillis, TimeUnit.MILLISECONDS);

        log.info("Ack flusher started: batches of {} or every {}ms, whichever comes first",
                flushSize, flushIntervalMillis);
    }

    /**
     * Records a broker acknowledgement. Called from the Kafka sender thread, so it must never block,
     * never allocate unboundedly and never touch the database.
     *
     * @param eventId the Mongo {@code _id} of the acknowledged event
     */
    public void acknowledge(String eventId) {
        if (eventId == null) {
            return;
        }
        acknowledged.add(eventId);
        metrics.setAckQueueDepth(depth.incrementAndGet());

        // Reaching the batch size only *schedules* the flush; the write still happens on the
        // flusher thread. Submitting is a lock-free queue append inside the executor.
        if (depth.get() >= flushSize) {
            submit(this::flushQuietly);
        }
    }

    /**
     * Runs a task on the flusher thread.
     *
     * <p>Offered so that the relay can move its failure handling — backoff arithmetic and a
     * {@code markForRetry} write — off the sender thread too. Failures are rare enough that they do
     * not need their own thread, and sharing this one keeps the guarantee simple to state: nothing
     * in this service does I/O on a producer callback.
     */
    public void submit(Runnable task) {
        ScheduledExecutorService executor = flusher;
        if (executor == null || executor.isShutdown()) {
            return;
        }
        try {
            executor.execute(task);
        } catch (RuntimeException e) {
            log.warn("Could not schedule work on the ack flusher: {}", e.getMessage());
        }
    }

    /** Current queue depth. Non-zero and growing means Mongo is not keeping up with the broker. */
    public int queueDepth() {
        return depth.get();
    }

    /**
     * Drains the queue in batches until it is empty.
     *
     * <p>The guard means a flush triggered by size while a timed flush is already running is simply
     * dropped, not queued behind it. That is deliberate — the work is idempotent and the running
     * flush will pick up whatever arrived — but it does mean the loop must re-check the queue after
     * releasing the guard, or the last few ids could sit until the next tick.
     */
    private void flush() {
        if (!flushing.compareAndSet(false, true)) {
            return;
        }
        try {
            drainAll();
        } finally {
            flushing.set(false);
        }

        // Anything that arrived while the guard was held.
        if (!acknowledged.isEmpty()) {
            submit(this::flushQuietly);
        }
    }

    private void drainAll() {
        List<String> batch = new ArrayList<>(flushSize);

        while (true) {
            batch.clear();
            for (int i = 0; i < flushSize; i++) {
                String id = acknowledged.poll();
                if (id == null) {
                    break;
                }
                batch.add(id);
            }
            if (batch.isEmpty()) {
                return;
            }

            depth.addAndGet(-batch.size());
            markPublished(batch);
        }
    }

    private void markPublished(List<String> batch) {
        try {
            int marked = repository.markPublishedBatch(batch, Instant.now(clock));
            metrics.recordAckFlushed(marked);
            log.debug("Marked {} event(s) PUBLISHED in one write", marked);
        } catch (RuntimeException e) {
            // Losing the mark is survivable and losing the service is not. These documents stay in
            // PUBLISHING until their lease expires, at which point the recovery worker republishes
            // them; consumers dedupe on wamid. Re-queueing the ids instead would grow the queue
            // without bound for as long as Mongo is unavailable.
            log.error("Could not mark {} event(s) PUBLISHED. They will be reclaimed once their "
                    + "lease expires and republished; consumers dedupe on wamid.", batch.size(), e);
        }
    }

    private void flushQuietly() {
        try {
            flush();
        } catch (RuntimeException e) {
            // A scheduled task that throws is silently unscheduled forever.
            log.error("Ack flush failed; the schedule continues", e);
        }
    }

    /**
     * Drains what is left before the process exits.
     *
     * <p>Skipping this would not lose an event — the lease expires and the recovery worker
     * republishes — but every rolling deploy would emit a duplicate burst the size of one flush
     * window, for no reason.
     */
    @PreDestroy
    void stop() {
        ScheduledExecutorService executor = flusher;
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }

        int remaining = depth.get();
        if (remaining > 0) {
            log.info("Flushing {} outstanding acknowledgement(s) before shutdown", remaining);
            flushing.set(false);
            try {
                drainAll();
            } catch (RuntimeException e) {
                log.warn("Final ack flush did not complete: {}", e.getMessage());
            }
        }
    }
}
