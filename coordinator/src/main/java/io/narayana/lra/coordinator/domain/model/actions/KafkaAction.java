package io.narayana.lra.coordinator.domain.model.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.contracts.kafka.KafkaActionBody;
import io.narayana.lra.contracts.kafka.LRAKafkaConstants;
import io.narayana.lra.coordinator.api.KafkaActionProducer;
import io.narayana.lra.logging.LRALogger;
import org.eclipse.microprofile.config.ConfigProvider;

public class KafkaAction implements LRAAction {
    private final String topic;
    private final String targetUid;

    @JsonCreator
    public KafkaAction(
            @JsonProperty("topic") String topic,
            @JsonProperty("targetUid") String targetUid) {
        this.topic = topic;
        this.targetUid = targetUid;
    }

    public KafkaAction(String topic) {
        this(topic, null);
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public ActionResult call(ActionContext context) {
        KafkaActionProducer producer = KafkaActionProducer.getInstance();
        if (producer == null) {
            LRALogger.logger.error("KafkaActionProducer not initialized - cannot send Kafka message");
            return new ActionResult(ActionStatus.ERROR);
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            KafkaActionBody body = new KafkaActionBody(context.getLraId(), context.getRecoveryId(),
                    context.getCompensatorData());
            String json = mapper.writeValueAsString(body);
            boolean success = producer.send(topic, context.getLraId(), json);
            return new ActionResult(success ? ActionStatus.OK : ActionStatus.ERROR);
        } catch (Exception e) {
            LRALogger.logger.errorf("Failed to serialize Kafka action payload: %s", e.getMessage());
            return new ActionResult(ActionStatus.ERROR);
        }
    }

    @Override
    public String extractTargetUid() {
        if (targetUid != null && isCoordinatorTopic(topic)) {
            return targetUid;
        }
        return null;
    }

    private static boolean isCoordinatorTopic(String topic) {
        try {
            String coordinatorTopic = ConfigProvider.getConfig()
                    .getOptionalValue(LRAKafkaConstants.CONFIG_SERVICE_ID, String.class)
                    .orElse(LRAKafkaConstants.TOPIC_REQUEST);
            return coordinatorTopic.equals(topic);
        } catch (Exception e) {
            return LRAKafkaConstants.TOPIC_REQUEST.equals(topic);
        }
    }

    @Override
    public String toJson() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize KafkaAction", e);
        }
    }
}