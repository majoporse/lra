/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.client.CallbackReceiver;
import io.narayana.lra.connectors.common.config.CallbackConfig;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.ParticipantRequest;
import io.narayana.lra.connectors.kafka.contracts.AfterLRAKafkaCallback;
import io.narayana.lra.connectors.kafka.contracts.CompensateKafkaCallback;
import io.narayana.lra.connectors.kafka.contracts.CompleteKafkaCallback;
import io.narayana.lra.connectors.kafka.contracts.ForgetKafkaCallback;
import io.narayana.lra.connectors.kafka.contracts.KafkaCallbackReply;
import io.narayana.lra.connectors.kafka.contracts.KafkaRequest;
import io.narayana.lra.connectors.kafka.contracts.LRAKafkaEnvelope;
import io.narayana.lra.connectors.kafka.contracts.StatusKafkaCallback;
import io.narayana.lra.connectors.kafka.outbound.KafkaOutbound;
import io.narayana.lra.connectors.kafka.support.LRAKafkaConstants;
import io.narayana.lra.logging.LRALogger;
import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
@IfBuildProperty(name = CallbackConfig.PROTOCOL_PROPERTY, stringValue = CallbackConfig.KAFKA)
public class KafkaInboundListener {

    @Inject
    KafkaOutbound kafkaOutbound;

    @Inject
    CallbackReceiver callbackReceiver;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Incoming(LRAKafkaConstants.TOPIC_REPLY)
    @Blocking(ordered = false)
    public CompletionStage<Void> onMessage(Message<String> message) {
        String payload = message.getPayload();

        try {
            LRAKafkaEnvelope envelope = asCallbackEnvelope(payload);

            if (envelope == null) {
                kafkaOutbound.completeReply(payload);
            } else {
                handleCallback(envelope);
            }
        } catch (Exception e) {
            LRALogger.logger.errorf("Failed to process Kafka message on service topic: %s",
                    e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }

        return message.ack();
    }

    private LRAKafkaEnvelope asCallbackEnvelope(String payload) throws Exception {
        JsonNode node = objectMapper.readTree(payload);

        if (!node.hasNonNull("type") || !LRAKafkaConstants.isCallbackRequest(node.get("type").asText())) {
            return null;
        }

        return objectMapper.treeToValue(node, LRAKafkaEnvelope.class);
    }

    private void handleCallback(LRAKafkaEnvelope envelope) throws Exception {
        CallbackType callbackType = callbackType(envelope.type);
        ParticipantRequest request = objectMapper.convertValue(envelope.payload, requestClass(callbackType));
        ParticipantReply reply = callbackReceiver.receive(callbackType, request);
        KafkaCallbackReply kafkaReply = toKafkaReply(callbackType, request, reply);

        kafkaOutbound.send(replyTopic(request), kafkaReply);
    }

    private String replyTopic(ParticipantRequest request) {
        if (request instanceof KafkaRequest<?> kafkaRequest
                && kafkaRequest.getReplyTopic() != null
                && !kafkaRequest.getReplyTopic().isBlank()) {
            return kafkaRequest.getReplyTopic();
        }

        return kafkaOutbound.coordinatorTopic();
    }

    private KafkaCallbackReply toKafkaReply(CallbackType operation, ParticipantRequest request,
            ParticipantReply reply) {
        String correlationId = request instanceof KafkaRequest<?> kafkaRequest
                ? kafkaRequest.getCorrelationId()
                : "";

        return switch (operation) {
            case COMPENSATE -> new CompensateKafkaCallback.Reply(correlationId, reply.status, reply.error);
            case COMPLETE -> new CompleteKafkaCallback.Reply(correlationId, reply.status, reply.error);
            case STATUS -> new StatusKafkaCallback.Reply(correlationId, reply.status, reply.error);
            case FORGET -> new ForgetKafkaCallback.Reply(correlationId, reply.error);
            case AFTER -> new AfterLRAKafkaCallback.Reply(correlationId, reply.error);
        };
    }

    private static CallbackType callbackType(String messageType) {
        return switch (messageType) {
            case LRAKafkaConstants.TYPE_COMPENSATE -> CallbackType.COMPENSATE;
            case LRAKafkaConstants.TYPE_COMPLETE -> CallbackType.COMPLETE;
            case LRAKafkaConstants.TYPE_STATUS -> CallbackType.STATUS;
            case LRAKafkaConstants.TYPE_FORGET -> CallbackType.FORGET;
            case LRAKafkaConstants.TYPE_AFTER_LRA -> CallbackType.AFTER;
            default -> throw new IllegalArgumentException("Unknown Kafka callback type: " + messageType);
        };
    }

    private static Class<? extends ParticipantRequest> requestClass(CallbackType operation) {
        return switch (operation) {
            case COMPENSATE -> CompensateKafkaCallback.Request.class;
            case COMPLETE -> CompleteKafkaCallback.Request.class;
            case STATUS -> StatusKafkaCallback.Request.class;
            case FORGET -> ForgetKafkaCallback.Request.class;
            case AFTER -> AfterLRAKafkaCallback.Request.class;
        };
    }
}
