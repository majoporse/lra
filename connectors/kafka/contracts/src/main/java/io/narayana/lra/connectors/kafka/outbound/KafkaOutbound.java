/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.kafka.outbound;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.connectors.kafka.contracts.KafkaMessage;
import io.narayana.lra.connectors.kafka.contracts.KafkaReply;
import io.narayana.lra.connectors.kafka.contracts.KafkaRequest;
import io.narayana.lra.connectors.kafka.contracts.LRAKafkaEnvelope;
import io.narayana.lra.connectors.kafka.support.LRAKafkaConstants;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * The shared Kafka message sender: envelope sending, bare reply sending and reply correlation.
 * <p>
 * The caller owns the message semantics (a coordinator request, a callback request or a reply).
 * This class owns only the envelope shape, the per-message topic and the correlation map.
 */
@ApplicationScoped
public class KafkaOutbound {

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    private final ConcurrentHashMap<String, PendingReply> pendingReplies = new ConcurrentHashMap<>();

    @Channel(LRAKafkaConstants.CHANNEL_OUTBOUND)
    Emitter<String> emitter;

    /**
     * Send a request envelope and wait for its correlated reply.
     *
     * @return the reply, or {@code null} on timeout
     */
    public <R extends KafkaReply> R request(String topic, KafkaRequest<R> request, Duration timeout) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        String correlationId = request.getCorrelationId();
        pendingReplies.put(correlationId, new PendingReply(request.getReplyType(), future));

        try {
            send(topic, request);
        } catch (RuntimeException e) {
            pendingReplies.remove(correlationId);
            throw e;
        }

        try {
            return request.getReplyType().cast(future.get(timeout.toMillis(), TimeUnit.MILLISECONDS));
        } catch (TimeoutException e) {
            pendingReplies.remove(correlationId);
            return null;
        } catch (InterruptedException e) {
            pendingReplies.remove(correlationId);
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            pendingReplies.remove(correlationId);
            Throwable cause = e.getCause();
            throw new IllegalStateException("Kafka request failed"
                    + (cause != null ? ": " + cause.getMessage() : ""), e);
        }
    }

    /**
     * Send an envelope without waiting for a reply.
     */
    public void send(String topic, KafkaMessage message) {
        emit(topic, serialise(new LRAKafkaEnvelope(message.getType(), message)));
    }

    /**
     * Send a bare payload, without an envelope. Coordinator replies use this shape.
     */
    public void sendRaw(String topic, KafkaReply reply) {
        emit(topic, serialise(reply));
    }

    /**
     * Complete the pending round trip from an inbound reply.
     * <p>
     * A callback reply arrives as an envelope; a coordinator reply arrives as the bare reply
     * object. Both carry a {@code correlationId}.
     */
    public void completeReply(String rawJson) {
        try {
            correlate(objectMapper.readTree(rawJson));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to read Kafka reply", e);
        }
    }

    /**
     * The coordinator's single inbound topic, used as the reply topic of a callback request and
     * as the destination of a participant request.
     */
    public String coordinatorTopic() {
        return ConfigProvider.getConfig()
                .getOptionalValue(LRAKafkaConstants.CONFIG_COORDINATOR_TOPIC, String.class)
                .orElse(LRAKafkaConstants.TOPIC_REQUEST);
    }

    private void correlate(JsonNode root) {
        JsonNode reply = LRAKafkaConstants.isCallbackReply(text(root, "type"))
                ? root.get("payload")
                : root;

        if (reply == null || !reply.isObject()) {
            return;
        }

        String correlationId = text(reply, "correlationId");

        if (correlationId == null) {
            return;
        }

        PendingReply pending = pendingReplies.remove(correlationId);

        if (pending == null) {
            return;
        }

        try {
            pending.future().complete(objectMapper.convertValue(reply, pending.replyType()));
        } catch (IllegalArgumentException e) {
            pending.future().completeExceptionally(e);
        }
    }

    private void emit(String topic, String json) {
        OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String> builder()
                .withTopic(topic)
                .build();

        Message<String> message = Message.of(json).addMetadata(metadata);
        emitter.send(message);
    }

    private String serialise(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize Kafka message", e);
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);

        return value == null || value.isNull() ? null : value.asText();
    }

    private record PendingReply(Class<? extends KafkaReply> replyType, CompletableFuture<Object> future) {
    }
}
