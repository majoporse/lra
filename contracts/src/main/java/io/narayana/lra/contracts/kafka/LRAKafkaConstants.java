package io.narayana.lra.contracts.kafka;

public final class LRAKafkaConstants {

    private LRAKafkaConstants() {
    }

    // Kafka topic/channel names
    public static final String TOPIC_REQUEST = "lra-request";
    public static final String TOPIC_REPLY = "lra-reply";
    public static final String TOPIC_REPLY_PREFIX = "lra-reply-";

    // Message types for the request envelope
    public static final String TYPE_START = "start";
    public static final String TYPE_CLOSE = "close";
    public static final String TYPE_CANCEL = "cancel";
    public static final String TYPE_LEAVE = "leave";
    public static final String TYPE_JOIN = "join";
    public static final String TYPE_STATUS = "status";

    // Config keys
    public static final String CONFIG_SERVICE_ID = "quarkus.lra.client-topic";
}
