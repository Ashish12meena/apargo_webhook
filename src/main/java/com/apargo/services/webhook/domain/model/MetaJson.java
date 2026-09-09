package com.apargo.services.webhook.domain.model;

/**
 * The structural keys of a Meta webhook body — the shape of the envelope, not the meaning of what it
 * carries.
 *
 * <p>{@link MetaField} holds the <em>values</em> of a change's {@code field}; this class holds the
 * <em>names</em> the splitter walks to find them. They are separated because they change for
 * different reasons: Meta adds new {@code field} values constantly, and has never renamed
 * {@code entry} or {@code changes}.
 *
 * <p>Every key here is read null-safely. A missing key must never become a 500, because enough 5xx
 * responses disable the subscription for every service on the platform.
 */
public final class MetaJson {

    // --- Envelope ----------------------------------------------------------
    public static final String ENTRY = "entry";
    public static final String CHANGES = "changes";
    public static final String FIELD = "field";
    public static final String VALUE = "value";
    public static final String ID = "id";

    // --- Metadata ----------------------------------------------------------
    public static final String METADATA = "metadata";
    public static final String PHONE_NUMBER_ID = "phone_number_id";

    // --- The two arrays under a "messages" change --------------------------
    /** Inbound customer messages. */
    public static final String MESSAGES = "messages";

    /** Delivery receipts. Structurally different from {@link #MESSAGES}, same {@code field}. */
    public static final String STATUSES = "statuses";

    public static final String FROM = "from";

    // --- Lane-specific partition key sources -------------------------------
    public static final String USER_PREFERENCES = "user_preferences";
    public static final String WA_ID = "wa_id";
    public static final String MESSAGE_TEMPLATE_ID = "message_template_id";
    public static final String MESSAGE_TEMPLATE_NAME = "message_template_name";

    // --- Synthetic keys for the oversized-body document --------------------
    /** Not a Meta field. Marks the single document stored when a body exceeds the ceiling. */
    public static final String TRUNCATED_FIELD = "__oversized__";

    public static final String TRUNCATED_PAYLOAD_KEY = "rawBodyPrefix";
    public static final String TRUNCATED_ORIGINAL_SIZE_KEY = "originalSizeBytes";

    private MetaJson() {
    }
}
