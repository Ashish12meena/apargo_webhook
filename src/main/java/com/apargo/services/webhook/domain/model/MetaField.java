package com.apargo.services.webhook.domain.model;

/**
 * The subscribable {@code field} values on a Meta {@code change}, as documented by the WhatsApp
 * Business Platform.
 *
 * <p>These are matched, never validated. An unrecognised field is routed to {@link Lane#OTHER}, not
 * rejected — Meta adds fields between versions without notice.
 */
public final class MetaField {

    public static final String MESSAGES = "messages";
    public static final String USER_PREFERENCES = "user_preferences";
    public static final String TEMPLATE_CATEGORY_UPDATE = "template_category_update";
    public static final String SECURITY = "security";
    public static final String BUSINESS_CAPABILITY_UPDATE = "business_capability_update";

    /** Covers message_template_status_update, _quality_update and _components_update. */
    public static final String TEMPLATE_PREFIX = "message_template";

    /** Covers phone_number_quality_update and phone_number_name_update. */
    public static final String PHONE_NUMBER_PREFIX = "phone_number_";

    /** Covers account_update, account_review_update and account_alerts. */
    public static final String ACCOUNT_PREFIX = "account_";

    private MetaField() {
    }
}
