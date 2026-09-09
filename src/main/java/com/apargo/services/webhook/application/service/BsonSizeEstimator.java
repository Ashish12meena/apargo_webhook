package com.apargo.services.webhook.application.service;

import com.apargo.services.webhook.domain.model.WebhookEvent;
import java.util.Collection;
import java.util.Map;

/**
 * Approximates the serialised BSON size of an event.
 *
 * <p>Used only for the ingest batch's byte trigger. The trigger has to measure BSON rather than raw
 * request bytes: the same payload is materially larger once it is a document with field names,
 * type bytes and length prefixes, and a limit set against request size would not fire when the
 * operator expects it to. Getting that wrong in the other direction is worse — a batch that exceeds
 * Mongo's 16MB command limit fails as a whole and takes every request in it down with it.
 *
 * <p>This is an estimate, and deliberately a cheap one. Actually encoding each document to measure
 * it would double the conversion work on the one path where Meta is waiting, to decide a threshold
 * whose default already sits at half the hard limit. The estimate runs high rather than low, so the
 * error is in the safe direction.
 */
final class BsonSizeEstimator {

    /** Document header plus terminator. */
    private static final int DOCUMENT_OVERHEAD = 5;

    /** Type byte, name and null terminator for one element, averaged. */
    private static final int ELEMENT_OVERHEAD = 12;

    /** Bytes per character, allowing for UTF-8 multi-byte sequences in message text. */
    private static final int BYTES_PER_CHAR = 2;

    /** Fixed-width scalars: long, double, ObjectId, date, boolean all sit at or under this. */
    private static final int SCALAR_SIZE = 12;

    /** Non-payload fields on the document: ids, timestamps, state, routing, error text. */
    private static final int FIXED_EVENT_OVERHEAD = 512;

    private BsonSizeEstimator() {
    }

    static long estimate(WebhookEvent event) {
        if (event == null) {
            return FIXED_EVENT_OVERHEAD;
        }
        long size = FIXED_EVENT_OVERHEAD;
        size += estimateValue(event.payload());
        size += estimateValue(event.wamids());
        size += textSize(event.topic()) + textSize(event.partitionKey()) + textSize(event.field());
        return size;
    }

    static long estimate(Collection<WebhookEvent> events) {
        if (events == null) {
            return 0;
        }
        long total = 0;
        for (WebhookEvent event : events) {
            total += estimate(event);
        }
        return total;
    }

    private static long estimateValue(Object value) {
        if (value == null) {
            return SCALAR_SIZE;
        }
        if (value instanceof String text) {
            return textSize(text);
        }
        if (value instanceof Map<?, ?> map) {
            long size = DOCUMENT_OVERHEAD;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                size += ELEMENT_OVERHEAD
                        + textSize(String.valueOf(entry.getKey()))
                        + estimateValue(entry.getValue());
            }
            return size;
        }
        if (value instanceof Collection<?> collection) {
            long size = DOCUMENT_OVERHEAD;
            for (Object element : collection) {
                size += ELEMENT_OVERHEAD + estimateValue(element);
            }
            return size;
        }
        return SCALAR_SIZE;
    }

    private static long textSize(String text) {
        return text == null ? SCALAR_SIZE : (long) text.length() * BYTES_PER_CHAR + DOCUMENT_OVERHEAD;
    }
}
