package io.narayana.lra.contracts.kafka;

import java.util.Set;

public final class LRAKafkaConstants {

    private LRAKafkaConstants() {
    }

    // Kafka topic/channel names
    public static final String TOPIC_REQUEST = "lra-request";
    public static final String TOPIC_REPLY = "lra-reply";
    public static final String TOPIC_REPLY_PREFIX = "lra-reply-";

    public static final String CHANNEL_CALLBACK = "lra-callback";

    /**
     * @deprecated the coordinator now receives callback replies on its single inbound topic
     *             ({@code quarkus.lra.coordinator-topic}, default {@link #TOPIC_REQUEST}).
     */
    @Deprecated
    public static final String TOPIC_CALLBACK_REPLY = "lra-callback-reply";

    // Config keys
    public static final String CONFIG_SERVICE_ID = "quarkus.lra.client-topic";
    public static final String CONFIG_COORDINATOR_TOPIC = "quarkus.lra.coordinator-topic";

    /**
     * @deprecated see {@link #TOPIC_CALLBACK_REPLY}
     */
    @Deprecated
    public static final String CONFIG_COORDINATOR_CALLBACK_TOPIC = "quarkus.lra.coordinator-callback-topic";

    public static final String TYPE_COMPENSATE = "compensate";
    public static final String TYPE_COMPLETE = "complete";
    public static final String TYPE_STATUS = "status";
    public static final String TYPE_FORGET = "forget";
    public static final String TYPE_AFTER_LRA = "afterLRA";

    public static final String TYPE_COMPENSATE_REPLY = "compensate-reply";
    public static final String TYPE_COMPLETE_REPLY = "complete-reply";
    public static final String TYPE_STATUS_REPLY = "status-reply";
    public static final String TYPE_FORGET_REPLY = "forget-reply";
    public static final String TYPE_AFTER_LRA_REPLY = "afterLRA-reply";

    private static final Set<String> CALLBACK_REQUESTS = Set.of(
            TYPE_COMPENSATE, TYPE_COMPLETE, TYPE_STATUS, TYPE_FORGET, TYPE_AFTER_LRA);

    private static final Set<String> CALLBACK_REPLIES = Set.of(
            TYPE_COMPENSATE_REPLY, TYPE_COMPLETE_REPLY, TYPE_STATUS_REPLY, TYPE_FORGET_REPLY,
            TYPE_AFTER_LRA_REPLY);

    public static boolean isCallbackRequest(String type) {
        return type != null && CALLBACK_REQUESTS.contains(type);
    }

    public static boolean isCallbackReply(String type) {
        return type != null && CALLBACK_REPLIES.contains(type);
    }

    public static String callbackReplyType(String callbackRequestType) {
        return callbackRequestType + "-reply";
    }
}
