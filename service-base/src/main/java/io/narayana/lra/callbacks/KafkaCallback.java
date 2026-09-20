package io.narayana.lra.callbacks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.callbacks.contracts.common.ParticipantReply;
import io.narayana.lra.callbacks.contracts.common.ParticipantRequest;
import io.narayana.lra.callbacks.contracts.kafka.AfterLRAKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.CompensateKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.CompleteKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.ForgetKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.StatusKafkaCallback;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.inject.spi.CDI;
import java.net.URI;
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
        COMPENSATE(CompensateKafkaCallback.TYPE),
        COMPLETE(CompleteKafkaCallback.TYPE),
        STATUS(StatusKafkaCallback.TYPE),
        FORGET(ForgetKafkaCallback.TYPE),
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
        String lraId = context.getLraId();
        URI lraUri = lraId == null ? null : URI.create(lraId);
        URI parentUri = context.getParentId() == null ? null : URI.create(context.getParentId());
        String recoveryUrl = context.getRecoveryId();
        String compensatorData = context.getCompensatorData();

        switch (operation) {
            case COMPENSATE:
                return new CompensateKafkaCallback.Request(correlationId, replyTopic, lraUri, parentUri, recoveryUrl,
                        compensatorData);
            case COMPLETE:
                return new CompleteKafkaCallback.Request(correlationId, replyTopic, lraUri, parentUri, recoveryUrl,
                        compensatorData);
            case STATUS:
                return new StatusKafkaCallback.Request(correlationId, replyTopic, lraUri, parentUri, recoveryUrl,
                        compensatorData);
            case FORGET:
                return new ForgetKafkaCallback.Request(correlationId, replyTopic, lraUri, parentUri, recoveryUrl,
                        compensatorData);
            case AFTER_LRA:
                String payload = context.getPayload();
                return new AfterLRAKafkaCallback.Request(correlationId, replyTopic, lraUri, parentUri,
                        recoveryUrl, compensatorData,
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
            return switch (operation) {
                case COMPENSATE -> mapEndResult(objectMapper.readValue(replyJson, CompensateKafkaCallback.Reply.class));
                case COMPLETE -> mapEndResult(objectMapper.readValue(replyJson, CompleteKafkaCallback.Reply.class));
                case STATUS -> mapStatusResult(objectMapper.readValue(replyJson, StatusKafkaCallback.Reply.class));
                case FORGET -> mapAckResult(objectMapper.readValue(replyJson, ForgetKafkaCallback.Reply.class).getError());
                case AFTER_LRA -> mapAckResult(objectMapper.readValue(replyJson, AfterLRAKafkaCallback.Reply.class).getError());
                default -> new CallbackResult(CallbackStatus.ERROR);
            };
        } catch (Exception e) {
            LRALogger.logger.errorf("Failed to deserialize Kafka callback reply (topic=%s, operation=%s): %s",
                    topic, operation, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            return new CallbackResult(CallbackStatus.ERROR);
        }
    }

    private CallbackResult mapEndResult(CompensateKafkaCallback.Reply reply) {
        return mapEndResult(reply, reply.getError());
    }

    private CallbackResult mapEndResult(CompleteKafkaCallback.Reply reply) {
        return mapEndResult(reply, reply.getError());
    }

    private CallbackResult mapEndResult(ParticipantReply reply, String error) {
        if (error != null || reply.status == null) {
            return new CallbackResult(CallbackStatus.ERROR, error);
        }

        return switch (reply.status) {
            case Completed, Compensated -> new CallbackResult(CallbackStatus.OK);
            case Completing, Compensating -> new CallbackResult(CallbackStatus.ACCEPTED);
            case FailedToComplete, FailedToCompensate -> new CallbackResult(CallbackStatus.FAILED, reply.status.name());
            default -> new CallbackResult(CallbackStatus.ERROR);
        };
    }

    private CallbackResult mapStatusResult(StatusKafkaCallback.Reply reply) {
        if (reply.getError() != null || reply.status == null) {
            return new CallbackResult(CallbackStatus.ERROR, reply.getError());
        }

        // a definite outcome is reported as OK with the status in the body, leaving
        // it in progress maps to ACCEPTED so recovery keeps polling
        return switch (reply.status) {
            case Completed, Compensated -> new CallbackResult(CallbackStatus.OK, reply.status.name());
            case FailedToComplete, FailedToCompensate -> new CallbackResult(CallbackStatus.OK, reply.status.name());
            default -> new CallbackResult(CallbackStatus.ACCEPTED);
        };
    }

    private CallbackResult mapAckResult(String error) {
        return error == null ? new CallbackResult(CallbackStatus.OK) : new CallbackResult(CallbackStatus.ERROR, error);
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
