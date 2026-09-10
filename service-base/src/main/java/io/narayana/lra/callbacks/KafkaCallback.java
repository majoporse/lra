package io.narayana.lra.callbacks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.logging.LRALogger;

public class KafkaCallback implements LRACallback {
    private final String topic;
    private final String targetUid;

    @JsonProperty("type")
    public String getType() {
        return "kafka";
    }

    @JsonCreator
    public KafkaCallback(@JsonProperty("topic") String topic, @JsonProperty("targetUid") String targetUid) {
        this.topic = topic;
        this.targetUid = targetUid;
    }

    public KafkaCallback(String topic) {
        this(topic, null);
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public CallbackResult call(CallbackContext context) {
        LRALogger.logger.warnf("KafkaCallback.call not yet wired up (topic=%s)", topic);
        return new CallbackResult(CallbackStatus.ERROR);
    }

    @Override
    public String extractTargetUid() {
        return targetUid;
    }

    @Override
    public String toJson() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize KafkaCallback", e);
        }
    }
}
