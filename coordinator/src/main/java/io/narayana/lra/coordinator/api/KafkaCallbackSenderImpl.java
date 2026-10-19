package io.narayana.lra.coordinator.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.callbacks.CallbackFactory;
import io.narayana.lra.callbacks.KafkaCallbackSender;
import io.narayana.lra.callbacks.Protocol;
import io.narayana.lra.callbacks.contracts.kafka.AfterLRAKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.CompensateKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.CompleteKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.ForgetKafkaCallback;
import io.narayana.lra.callbacks.contracts.kafka.KafkaCallbackReply;
import io.narayana.lra.callbacks.contracts.kafka.KafkaCallbackRequest;
import io.narayana.lra.callbacks.contracts.kafka.StatusKafkaCallback;
import io.narayana.lra.contracts.kafka.LRAKafkaConstants;
import io.narayana.lra.contracts.kafka.LRAKafkaEnvelope;
import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
@IfBuildProperty(name = CallbackFactory.PROTOCOL_PROPERTY, stringValue = Protocol.KAFKA_VALUE)
public class KafkaCallbackSenderImpl implements KafkaCallbackSender {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentHashMap<String, CompletableFuture<KafkaCallbackReply>> pendingReplies = new ConcurrentHashMap<>();
    private final String replyTopic = resolveReplyTopic();

    @Channel(LRAKafkaConstants.CHANNEL_CALLBACK)
    Emitter<String> callbackEmitter;

    @Override
    public String getReplyTopic() {
        return replyTopic;
    }

    @Override
    public KafkaCallbackReply send(String topic, String type, KafkaCallbackRequest payload, String correlationId,
            long timeout, TimeUnit unit) throws InterruptedException {
        CompletableFuture<KafkaCallbackReply> future = new CompletableFuture<>();
        pendingReplies.put(correlationId, future);

        try {
            sendEnvelope(topic, type, payload);
            return future.get(timeout, unit);
        } catch (TimeoutException e) {
            pendingReplies.remove(correlationId);
            return null;
        } catch (ExecutionException e) {
            pendingReplies.remove(correlationId);
            Throwable cause = e.getCause();
            throw new RuntimeException("Kafka callback request failed" + (cause != null ? ": " + cause.getMessage()
                    : ""), e);
        }
    }

    public void handleReply(LRAKafkaEnvelope envelope) {
        KafkaCallbackReply reply = parseReply(envelope);

        if (reply == null) {
            return;
        }

        CompletableFuture<KafkaCallbackReply> future = pendingReplies.remove(reply.getCorrelationId());

        if (future != null) {
            future.complete(reply);
        }
    }

    private KafkaCallbackReply parseReply(LRAKafkaEnvelope envelope) {
        if (envelope == null || envelope.type == null) {
            return null;
        }

        return switch (envelope.type) {
            case LRAKafkaConstants.TYPE_COMPENSATE_REPLY ->
                objectMapper.convertValue(envelope.payload, CompensateKafkaCallback.Reply.class);
            case LRAKafkaConstants.TYPE_COMPLETE_REPLY ->
                objectMapper.convertValue(envelope.payload, CompleteKafkaCallback.Reply.class);
            case LRAKafkaConstants.TYPE_STATUS_REPLY ->
                objectMapper.convertValue(envelope.payload, StatusKafkaCallback.Reply.class);
            case LRAKafkaConstants.TYPE_FORGET_REPLY ->
                objectMapper.convertValue(envelope.payload, ForgetKafkaCallback.Reply.class);
            case LRAKafkaConstants.TYPE_AFTER_LRA_REPLY ->
                objectMapper.convertValue(envelope.payload, AfterLRAKafkaCallback.Reply.class);
            default -> null;
        };
    }

    private void sendEnvelope(String topic, String type, KafkaCallbackRequest payload) {
        try {
            LRAKafkaEnvelope envelope = new LRAKafkaEnvelope(type, payload);
            String envelopeJson = objectMapper.writeValueAsString(envelope);

            OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String> builder()
                    .withTopic(topic)
                    .build();

            Message<String> message = Message.of(envelopeJson).addMetadata(metadata);
            callbackEmitter.send(message);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize Kafka callback request for topic " + topic, e);
        }
    }

    private String resolveReplyTopic() {
        return ConfigProvider.getConfig()
                .getOptionalValue(LRAKafkaConstants.CONFIG_COORDINATOR_TOPIC, String.class)
                .orElse(LRAKafkaConstants.TOPIC_REQUEST);
    }
}
