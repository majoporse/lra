package io.narayana.lra.contracts.kafka;

public final class LRAKafkaConstants {

    private LRAKafkaConstants() {
    }

    // Kafka topic/channel names
    public static final String TOPIC_START = "lra-start";
    public static final String TOPIC_CLOSE = "lra-close";
    public static final String TOPIC_CANCEL = "lra-cancel";
    public static final String TOPIC_LEAVE = "lra-leave";
    public static final String TOPIC_JOIN = "lra-join";
    public static final String TOPIC_STATUS = "lra-status";
    public static final String TOPIC_REPLY = "lra-reply";
    public static final String TOPIC_REPLY_PREFIX = "lra-reply-";

    // Config keys
    public static final String CONFIG_TRANSPORT = "lra.transport";
    public static final String CONFIG_SERVICE_ID = "lra.service.id";
    public static final String CONFIG_REPLY_TOPIC = "lra.reply.topic";
}
