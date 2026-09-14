package io.narayana.lra.coordinator.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.callbacks.KafkaCallbackSender;
import io.narayana.lra.contracts.kafka.LRAKafkaConstants;
import io.narayana.lra.contracts.kafka.LRAKafkaEnvelope;
import io.narayana.lra.logging.LRALogger;
import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Kafka sender for {@link io.narayana.lra.callbacks.KafkaCallback}. Delivers
 * participant callback requests to the participant's listener topic and
 * correlates the participant's reply (published to the coordinator's callback
 * reply topic) by correlation id, mirroring the request/reply handling of
 * {@link KafkaLRAClient}.
 */
@ApplicationScoped
@IfBuildProperty(name = "quarkus.lra.kafka.enabled", stringValue = "true")
public class KafkaCallbackSenderImpl implements KafkaCallbackSender {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentHashMap<String, CompletableFuture<String>> pendingReplies = new ConcurrentHashMap<>();
    private final String replyTopic = resolveReplyTopic();

    @Channel(LRAKafkaConstants.CHANNEL_CALLBACK)
    Emitter<String> callbackEmitter;

    @Override
    public String getReplyTopic() {
        return replyTopic;
    }

    @Override
    public String send(String topic, String type, Object payload, String correlationId, long timeout, TimeUnit unit)
            throws InterruptedException {
        CompletableFuture<String> future = new CompletableFuture<>();
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

    @Incoming(LRAKafkaConstants.TOPIC_CALLBACK_REPLY)
    public CompletionStage<Void> onIncomingReply(Message<String> message) {
        String replyJson = message.getPayload();
        String correlationId = extractCorrelationIdFromPayload(replyJson);
        if (correlationId != null) {
            CompletableFuture<String> future = pendingReplies.remove(correlationId);
            if (future != null) {
                future.complete(replyJson);
            }
        }
        return message.ack();
    }

    private void sendEnvelope(String topic, String type, Object payload) {
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

    private String extractCorrelationIdFromPayload(String payload) {
        try {
            var node = objectMapper.readTree(payload);
            if (node.has("correlationId")) {
                return node.get("correlationId").asText();
            }
        } catch (Exception e) {
            LRALogger.logger.error("Failed to extract correlationId from Kafka callback reply", e);
        }
        return null;
    }

    private String resolveReplyTopic() {
        return ConfigProvider.getConfig()
                .getOptionalValue(LRAKafkaConstants.CONFIG_COORDINATOR_CALLBACK_TOPIC, String.class)
                .orElse(LRAKafkaConstants.TOPIC_CALLBACK_REPLY);
    }
}
