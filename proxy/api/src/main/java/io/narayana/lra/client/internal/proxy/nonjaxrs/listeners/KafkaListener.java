package io.narayana.lra.client.internal.proxy.nonjaxrs.listeners;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.Current;
import io.narayana.lra.callbacks.CallbackResult;
import io.narayana.lra.callbacks.contracts.kafka.AfterLRAKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.CompensateLRAKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.CompleteLRAKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.ForgetLraKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.StatusLraKafkaCallback;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipant;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipantRegistry;
import io.narayana.lra.contracts.kafka.LRAKafkaConstants;
import io.narayana.lra.contracts.kafka.LRAKafkaEnvelope;
import io.narayana.lra.logging.LRALogger;
import io.narayana.lra.proxy.logging.LRAProxyLogger;
import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
@IfBuildProperty(name = "quarkus.lra.client.protocol", stringValue = "kafka")
public class KafkaListener {

    private final ObjectMapper objectMapper;

    @Inject
    private LRAParticipantRegistry lraParticipantRegistry;

    @Channel(LRAKafkaConstants.CHANNEL_CALLBACK)
    Emitter<String> replyEmitter;

    public KafkaListener() {
        this.objectMapper = new ObjectMapper();
    }

    @Incoming(LRAKafkaConstants.TOPIC_REQUEST)
    public CompletionStage<Void> onRequest(Message<String> message) {
        String rawJson = message.getPayload();

        try {
            LRAKafkaEnvelope envelope = objectMapper.readValue(rawJson, LRAKafkaEnvelope.class);

            switch (envelope.type) {
                case CompleteLRAKafkaCallback.TYPE:
                    handleComplete(objectMapper.convertValue(envelope.payload, CompleteLRAKafkaCallback.Request.class));
                    break;
                case CompensateLRAKafkaCallback.TYPE:
                    handleCompensate(objectMapper.convertValue(envelope.payload, CompensateLRAKafkaCallback.Request.class));
                    break;
                case ForgetLraKafkaCallback.TYPE:
                    handleForget(objectMapper.convertValue(envelope.payload, ForgetLraKafkaCallback.Request.class));
                    break;
                case AfterLRAKafkaCallback.TYPE:
                    handleAfter(objectMapper.convertValue(envelope.payload, AfterLRAKafkaCallback.Request.class));
                    break;
                case StatusLraKafkaCallback.TYPE:
                    handleStatus(objectMapper.convertValue(envelope.payload, StatusLraKafkaCallback.Request.class));
                    break;
                default:
                    LRALogger.logger.error("Unknown LRA Kafka message type: " + envelope.type);
                    break;
            }
        } catch (Exception e) {
            LRALogger.logger.error("Failed to process LRA Kafka message", e);
        }

        return message.ack();
    }

    private void handleComplete(CompleteLRAKafkaCallback.Request request) {
        CallbackResult result = getParticipant(request.participantId).complete(
                Current.toURI(request.lraId), Current.toURI(request.parentId));
        sendReply(request.getReplyTopic(), new CompleteLRAKafkaCallback.Reply(request.getCorrelationId(), result));
    }

    private void handleCompensate(CompensateLRAKafkaCallback.Request request) {
        CallbackResult result = getParticipant(request.participantId).compensate(
                Current.toURI(request.lraId), Current.toURI(request.parentId));
        sendReply(request.getReplyTopic(), new CompensateLRAKafkaCallback.Reply(request.getCorrelationId(), result));
    }

    private void handleForget(ForgetLraKafkaCallback.Request request) {
        CallbackResult result = getParticipant(request.participantId).forget(
                Current.toURI(request.lraId), Current.toURI(request.parentId));
        sendReply(request.getReplyTopic(), new ForgetLraKafkaCallback.Reply(request.getCorrelationId(), result));
    }

    private void handleAfter(AfterLRAKafkaCallback.Request request) {
        CallbackResult result = getParticipant(request.participantId).afterLRA(
                Current.toURI(request.lraId), request.endStatus);
        sendReply(request.getReplyTopic(), new AfterLRAKafkaCallback.Reply(request.getCorrelationId(), result));
    }

    private void handleStatus(StatusLraKafkaCallback.Request request) {
        CallbackResult result = getParticipant(request.participantId).status(
                Current.toURI(request.lraId), Current.toURI(request.parentId));
        sendReply(request.getReplyTopic(), new StatusLraKafkaCallback.Reply(request.getCorrelationId(), result));
    }

    private void sendReply(String replyTopic, Object reply) {
        try {
            String replyJson = objectMapper.writeValueAsString(reply);

            OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String> builder()
                    .withTopic(replyTopic)
                    .build();

            Message<String> message = Message.of(replyJson).addMetadata(metadata);
            replyEmitter.send(message);
        } catch (JsonProcessingException e) {
            LRALogger.logger.error("Failed to serialize reply", e);
        }
    }

    private LRAParticipant getParticipant(String participantId) {
        LRAParticipant participant = lraParticipantRegistry.getParticipant(participantId);
        if (participant == null) {
            String errMsg = LRAProxyLogger.i18NLogger.error_missingParticipant(participantId);
            throw new WebApplicationException(errMsg, Response.status(Response.Status.NOT_FOUND)
                    .entity(errMsg)
                    .build());
        }
        return participant;
    }
}
