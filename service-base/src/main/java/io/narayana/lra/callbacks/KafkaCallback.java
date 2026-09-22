package io.narayana.lra.callbacks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.callbacks.contracts.common.ParticipantRequest;
import io.narayana.lra.callbacks.contracts.kafka.AfterLRAKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.CompensateLRAKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.CompleteLRAKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.ForgetLraKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.StatusLraKafkaCallback;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.inject.spi.CDI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

/**
 * A participant callback that is delivered to the participant's client-side
 * Kafka listener instead of an HTTP endpoint.
 */
public class KafkaCallback implements LRACallback {

    /**
     * The participant operation this callback represents. This determines both
     * the Kafka message type used for the notification and the payload contract
     * exchanged with the client-side listener.
     */
    public enum Operation {
        COMPENSATE(CompensateLRAKafkaCallback.TYPE),
        COMPLETE(CompleteLRAKafkaCallback.TYPE),
        STATUS(StatusLraKafkaCallback.TYPE),
        FORGET(ForgetLraKafkaCallback.TYPE),
        AFTER_LRA(AfterLRAKafkaCallback.TYPE);

        private final String messageType;

        Operation(String messageType) {
            this.messageType = messageType;
        }

        public String getMessageType() {
            return messageType;
        }

        public static Operation fromMessageType(String messageType) {
            for (Operation operation : values()) {
                if (operation.messageType.equals(messageType)) {
                    return operation;
                }
            }

            throw new IllegalArgumentException("Unknown Kafka participant message type: " + messageType);
        }
    }

    /**
     * Factory for a compensate (@Compensate) callback.
     */
    public static KafkaCallback compensateCallback(String topic, String targetUid) {
        return new KafkaCallback(topic, targetUid, Operation.COMPENSATE);
    }

    /**
     * Factory for a complete (@Complete) callback.
     */
    public static KafkaCallback completeCallback(String topic, String targetUid) {
        return new KafkaCallback(topic, targetUid, Operation.COMPLETE);
    }

    /**
     * Factory for a status (@Status) callback.
     */
    public static KafkaCallback statusCallback(String topic, String targetUid) {
        return new KafkaCallback(topic, targetUid, Operation.STATUS);
    }

    /**
     * Factory for a forget (@Forget) callback.
     */
    public static KafkaCallback forgetCallback(String topic, String targetUid) {
        return new KafkaCallback(topic, targetUid, Operation.FORGET);
    }

    /**
     * Factory for an after LRA (@AfterLRA) notification callback.
     */
    public static KafkaCallback afterCallback(String topic, String targetUid) {
        return new KafkaCallback(topic, targetUid, Operation.AFTER_LRA);
    }

    private final String topic;
    private final String targetUid;
    private final Operation operation;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @JsonCreator
    public KafkaCallback(
            @JsonProperty("topic") String topic,
            @JsonProperty("targetUid") String targetUid,
            @JsonProperty("operation") Operation operation) {
        this.topic = topic;
        this.targetUid = targetUid;
        this.operation = operation != null ? operation : Operation.COMPLETE;
    }

    public String getTopic() {
        return topic;
    }

    public Operation getOperation() {
        return operation;
    }

    /**
     * Map the callback context to the request payload that should be delivered
     * to the client-side Kafka listener for the represented operation.
     *
     * @param correlationId the id used to correlate the reply with this request
     * @param replyTopic the topic on which the caller receives the reply
     */
    public ParticipantRequest newRequest(CallbackContext context, String correlationId, String replyTopic) {
        UUID lraId = context.getLraId();
        UUID parentId = context.getParentId();
        String recoveryUrl = context.getRecoveryId();
        String compensatorData = context.getCompensatorData();
        String participantId = context.getParticipantId();

        switch (operation) {
            case COMPENSATE:
                return new CompensateLRAKafkaCallback.Request(correlationId, replyTopic, lraId, parentId, recoveryUrl,
                        compensatorData, participantId);
            case COMPLETE:
                return new CompleteLRAKafkaCallback.Request(correlationId, replyTopic, lraId, parentId, recoveryUrl,
                        compensatorData, participantId);
            case STATUS:
                return new StatusLraKafkaCallback.Request(correlationId, replyTopic, lraId, parentId, recoveryUrl,
                        compensatorData, participantId);
            case FORGET:
                return new ForgetLraKafkaCallback.Request(correlationId, replyTopic, lraId, parentId, recoveryUrl,
                        compensatorData, participantId);
            case AFTER_LRA:
                String payload = context.getPayload();
                return new AfterLRAKafkaCallback.Request(correlationId, replyTopic, lraId, parentId,
                        recoveryUrl, compensatorData, participantId,
                        payload == null ? null : LRAStatus.valueOf(payload));
            default:
                throw new IllegalStateException("Unexpected operation: " + operation);
        }
    }

    @Override
    public CallbackResult call(CallbackContext context) {
        KafkaCallbackSender sender = sender();
        if (sender == null) {
            LRALogger.logger.warnf("KafkaCallback.call not available (topic=%s, operation=%s): no Kafka sender "
                    + "configured", topic, operation);
            return new CallbackResult(CallbackStatus.ERROR);
        }

        String correlationId = UUID.randomUUID().toString();
        Object request = newRequest(context, correlationId, sender.getReplyTopic());

        try {
            String replyJson = sender.send(topic, operation.getMessageType(), request, correlationId,
                    LRAConstants.PARTICIPANT_TIMEOUT, TimeUnit.SECONDS);

            if (replyJson == null) {
                return new CallbackResult(CallbackStatus.TIMEOUT);
            }

            return mapReply(replyJson);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new CallbackResult(CallbackStatus.TIMEOUT);
        } catch (Exception e) {
            LRALogger.logger.errorf("KafkaCallback.call failed (topic=%s, operation=%s): %s",
                    topic, operation, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            return new CallbackResult(CallbackStatus.ERROR);
        }
    }

    private KafkaCallbackSender sender() {
        try {
            if (CDI.current().select(KafkaCallbackSender.class).isResolvable()) {
                return CDI.current().select(KafkaCallbackSender.class).get();
            }
        } catch (Exception e) {
            LRALogger.logger.debugf("CDI not available for Kafka callback sender lookup: %s", e.getMessage());
        }
        return null;
    }

    private CallbackResult mapReply(String replyJson) {
        try {
            CallbackResult result = switch (operation) {
                case COMPENSATE ->
                    objectMapper.readValue(replyJson, CompensateLRAKafkaCallback.Reply.class).result;
                case COMPLETE ->
                    objectMapper.readValue(replyJson, CompleteLRAKafkaCallback.Reply.class).result;
                case STATUS ->
                    objectMapper.readValue(replyJson, StatusLraKafkaCallback.Reply.class).result;
                case FORGET ->
                    objectMapper.readValue(replyJson, ForgetLraKafkaCallback.Reply.class).result;
                case AFTER_LRA ->
                    objectMapper.readValue(replyJson, AfterLRAKafkaCallback.Reply.class).result;
                default -> null;
            };

            return result != null ? result : new CallbackResult(CallbackStatus.ERROR);
        } catch (Exception e) {
            LRALogger.logger.errorf("Failed to deserialize Kafka callback reply (topic=%s, operation=%s): %s",
                    topic, operation, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            return new CallbackResult(CallbackStatus.ERROR);
        }
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
