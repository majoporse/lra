package io.narayana.lra.contracts.kafka;

public final class LRAKafkaConstants {

    private LRAKafkaConstants() {
    }

    // Kafka topic/channel names
    public static final String TOPIC_REQUEST = "lra-request";
    public static final String TOPIC_REPLY = "lra-reply";
    public static final String TOPIC_REPLY_PREFIX = "lra-reply-";

    // Kafka channels for participant callbacks: the coordinator sends callback
    // requests to the participant's listener topic and receives the reply on its
    // own callback reply topic (see quarkus.lra.coordinator-callback-topic).
    public static final String CHANNEL_CALLBACK = "lra-callback";
    public static final String TOPIC_CALLBACK_REPLY = "lra-callback-reply";

    // Config keys
    public static final String CONFIG_SERVICE_ID = "quarkus.lra.client-topic";
    public static final String CONFIG_COORDINATOR_CALLBACK_TOPIC = "quarkus.lra.coordinator-callback-topic";
}
