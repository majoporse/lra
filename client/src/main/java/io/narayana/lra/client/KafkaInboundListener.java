/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.api.LRA;
import io.narayana.lra.callbacks.CallbackFactory;
import io.narayana.lra.callbacks.CallbackType;
import io.narayana.lra.callbacks.KafkaCallback;
import io.narayana.lra.callbacks.ParticipantCallbackDispatcher;
import io.narayana.lra.callbacks.Protocol;
import io.narayana.lra.callbacks.contracts.common.ParticipantReply;
import io.narayana.lra.callbacks.contracts.common.ParticipantRequest;
import io.narayana.lra.callbacks.contracts.kafka.AfterLRAKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.CompensateKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.CompleteKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.ForgetKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.KafkaCallbackReply;
import io.narayana.lra.callbacks.contracts.kafka.KafkaCallbackRequest;
import io.narayana.lra.callbacks.contracts.kafka.StatusKafkaCallback;
import io.narayana.lra.contracts.kafka.LRAKafkaConstants;
import io.narayana.lra.contracts.kafka.LRAKafkaEnvelope;
import io.narayana.lra.logging.LRALogger;
import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
@IfBuildProperty(name = CallbackFactory.PROTOCOL_PROPERTY, stringValue = Protocol.KAFKA_VALUE)
public class KafkaInboundListener {

    @Inject
    KafkaLRAClient kafkaLRAClient;

    @Inject
    Instance<ParticipantCallbackDispatcher> participantCallbackDispatcher;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Incoming(LRAKafkaConstants.TOPIC_REPLY)
    @Blocking(ordered = false)
    public CompletionStage<Void> onMessage(Message<String> message) {
        String payload = message.getPayload();

        try {
            LRAKafkaEnvelope envelope = asCallbackEnvelope(payload);

            if (envelope == null) {
                kafkaLRAClient.handleReply(payload);
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
        KafkaCallback.Operation operation = KafkaCallback.Operation.fromMessageType(envelope.type);
        ParticipantRequest request = objectMapper.convertValue(envelope.payload, requestClass(operation));
        ParticipantReply reply = dispatch(operation, request);
        KafkaCallbackReply kafkaReply = toKafkaReply(operation, request, reply);
        LRAKafkaEnvelope replyEnvelope = new LRAKafkaEnvelope(
                LRAKafkaConstants.callbackReplyType(envelope.type), kafkaReply);

        kafkaLRAClient.sendToCoordinator(objectMapper.writeValueAsString(replyEnvelope));
    }

    private ParticipantReply dispatch(KafkaCallback.Operation operation, ParticipantRequest request) {
        if (!participantCallbackDispatcher.isResolvable()) {
            LRALogger.logger.warnf("No transport-neutral LRA participant dispatcher is available for %s %s",
                    operation, request.lraId);
            return null;
        }

        ParticipantReply reply = participantCallbackDispatcher.get().dispatch(callbackType(operation), request);

        if (reply == null) {
            LRALogger.logger.warnf("No transport-neutral LRA participant is registered for id %s (%s %s)",
                    request.participantId, operation, request.lraId);
        }

        return reply;
    }

    private KafkaCallbackReply toKafkaReply(KafkaCallback.Operation operation, ParticipantRequest request,
            ParticipantReply reply) {
        String correlationId = request instanceof KafkaCallbackRequest kafkaRequest
                ? kafkaRequest.getCorrelationId()
                : "";
        String error = reply == null ? "No LRA participant registered for id " + request.participantId : reply.error;
        LRA.ParticipantOutcome status = reply == null ? null : reply.status;

        return switch (operation) {
            case COMPENSATE -> new CompensateKafkaCallback.Reply(correlationId, status, error);
            case COMPLETE -> new CompleteKafkaCallback.Reply(correlationId, status, error);
            case STATUS -> new StatusKafkaCallback.Reply(correlationId, status, error);
            case FORGET -> new ForgetKafkaCallback.Reply(correlationId, error);
            case AFTER_LRA -> new AfterLRAKafkaCallback.Reply(correlationId, error);
        };
    }

    private static CallbackType callbackType(KafkaCallback.Operation operation) {
        return switch (operation) {
            case COMPENSATE -> CallbackType.COMPENSATE;
            case COMPLETE -> CallbackType.COMPLETE;
            case STATUS -> CallbackType.STATUS;
            case FORGET -> CallbackType.FORGET;
            case AFTER_LRA -> CallbackType.AFTER;
        };
    }

    private static Class<? extends ParticipantRequest> requestClass(KafkaCallback.Operation operation) {
        return switch (operation) {
            case COMPENSATE -> CompensateKafkaCallback.Request.class;
            case COMPLETE -> CompleteKafkaCallback.Request.class;
            case STATUS -> StatusKafkaCallback.Request.class;
            case FORGET -> ForgetKafkaCallback.Request.class;
            case AFTER_LRA -> AfterLRAKafkaCallback.Request.class;
        };
    }
}
