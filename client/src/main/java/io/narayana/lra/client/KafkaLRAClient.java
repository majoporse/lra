package io.narayana.lra.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.callbacks.ParticipantCallbacks;
import io.narayana.lra.contracts.kafka.CancelLRAKafka;
import io.narayana.lra.contracts.kafka.CloseLRAKafka;
import io.narayana.lra.contracts.kafka.JoinLRAKafka;
import io.narayana.lra.contracts.kafka.LRAKafkaConstants;
import io.narayana.lra.contracts.kafka.LRAKafkaEnvelope;
import io.narayana.lra.contracts.kafka.LRAKafkaReply;
import io.narayana.lra.contracts.kafka.LRAKafkaRequest;
import io.narayana.lra.contracts.kafka.LeaveLRAKafka;
import io.narayana.lra.contracts.kafka.StartLRAKafka;
import io.narayana.lra.contracts.kafka.StatusLRAKafka;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class KafkaLRAClient {
    private static final long REPLY_TIMEOUT_SECONDS = 30;
    private static final boolean FIRE_AND_FORGET = true;

    @Inject
    @Channel(LRAKafkaConstants.TOPIC_REQUEST)
    Emitter<String> requestEmitter;

    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<String, CompletableFuture<String>> pendingReplies;
    private final String replyTopic;

    public KafkaLRAClient() {
        this.objectMapper = new ObjectMapper();
        this.pendingReplies = new ConcurrentHashMap<>();
        this.replyTopic = resolveReplyTopic();
    }

    private String resolveReplyTopic() {
        String topic = ConfigProvider.getConfig()
                .getOptionalValue(LRAKafkaConstants.CONFIG_SERVICE_ID, String.class)
                .orElse(null);
        if (topic == null || topic.isEmpty()) {
            throw new IllegalStateException(
                    "Missing required config property '" + LRAKafkaConstants.CONFIG_SERVICE_ID
                            + "'. Each service must have a unique ID for Kafka reply routing.");
        }
        return topic;
    }

    @Incoming(LRAKafkaConstants.TOPIC_REPLY)
    public CompletionStage<Void> onReply(Message<String> message) {
        String correlationId = extractCorrelationIdFromPayload(message.getPayload());
        if (correlationId != null) {
            CompletableFuture<String> future = pendingReplies.remove(correlationId);
            if (future != null) {
                future.complete(message.getPayload());
            }
        }
        return message.ack();
    }

    private String extractCorrelationIdFromPayload(String payload) {
        try {
            var node = objectMapper.readTree(payload);
            if (node.has("correlationId")) {
                return node.get("correlationId").asText();
            }
        } catch (Exception e) {
            LRALogger.logger.error("Failed to extract correlationId from payload", e);
        }
        return null;
    }

    private <T extends LRAKafkaRequest, R extends LRAKafkaReply> R send(
            String type, T request, Class<R> replyClass, boolean fireAndForget) {
        try {
            LRAKafkaEnvelope envelope = new LRAKafkaEnvelope(type, request);
            String envelopeJson = objectMapper.writeValueAsString(envelope);

            if (fireAndForget) {
                requestEmitter.send(envelopeJson);
                return null;
            }
            return sendAndWait(request.getCorrelationId(), envelopeJson, replyClass);
        } catch (JsonProcessingException e) {
            throw new WebApplicationException("Failed to serialize request: " + e.getMessage(),
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    private <R extends LRAKafkaReply> R sendAndWait(
            String correlationId, String envelopeJson, Class<R> replyClass) {
        CompletableFuture<String> future = new CompletableFuture<>();
        pendingReplies.put(correlationId, future);

        requestEmitter.send(envelopeJson);

        try {
            String replyJson = future.get(REPLY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return objectMapper.readValue(replyJson, replyClass);
        } catch (TimeoutException e) {
            pendingReplies.remove(correlationId);
            throw new WebApplicationException("Kafka request timed out",
                    Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
        } catch (ExecutionException | InterruptedException e) {
            pendingReplies.remove(correlationId);
            throw new WebApplicationException("Kafka request failed: " + e.getMessage(),
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        } catch (JsonProcessingException e) {
            pendingReplies.remove(correlationId);
            throw new WebApplicationException("Failed to deserialize reply: " + e.getMessage(),
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    private String nextCorrelationId() {
        return UUID.randomUUID().toString();
    }

    private void checkError(LRAKafkaReply reply) {
        if (reply.getError() != null) {
            throw new WebApplicationException(reply.getError(),
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    //    @Override
    public URI startLRA(URI parentLRA, String clientID, Long timeout, ChronoUnit unit, boolean verbose) {
        Long timeoutMillis = timeout != null ? java.time.Duration.of(timeout, unit).toMillis() : 0L;

        StartLRAKafka.Request request = new StartLRAKafka.Request(nextCorrelationId(), replyTopic, clientID,
                timeoutMillis,
                parentLRA);
        StartLRAKafka.Reply reply = send(LRAKafkaConstants.TYPE_START, request, StartLRAKafka.Reply.class, false);

        checkError(reply);
        return reply.lraId;
    }

    //    @Override
    public void closeLRA(URI lraId, String compensator, String userData) {
        CloseLRAKafka.Request request = new CloseLRAKafka.Request(nextCorrelationId(), replyTopic,
                lraId, compensator, userData);
        send(LRAKafkaConstants.TYPE_CLOSE, request, CloseLRAKafka.Reply.class, FIRE_AND_FORGET);
    }

    //    @Override
    public void cancelLRA(URI lraId, String compensator, String userData) {
        CancelLRAKafka.Request request = new CancelLRAKafka.Request(nextCorrelationId(), replyTopic,
                lraId, compensator, userData);
        send(LRAKafkaConstants.TYPE_CANCEL, request, CancelLRAKafka.Reply.class, FIRE_AND_FORGET);
    }

    //    @Override
    public void leaveLRA(URI lraId, String body) {
        LeaveLRAKafka.Request request = new LeaveLRAKafka.Request(nextCorrelationId(), replyTopic,
                lraId, body);
        send(LRAKafkaConstants.TYPE_LEAVE, request, LeaveLRAKafka.Reply.class, FIRE_AND_FORGET);
    }

    //    @Override
    public URI joinLRA(URI lraId, Long timeLimit, ParticipantCallbacks callbacks,
            StringBuilder userData, String partId) {

        JoinLRAKafka.Request request = new JoinLRAKafka.Request(
                nextCorrelationId(), replyTopic, lraId, timeLimit, callbacks, userData.toString(), partId);

        JoinLRAKafka.Reply reply = send(LRAKafkaConstants.TYPE_JOIN, request, JoinLRAKafka.Reply.class, false);

        checkError(reply);

        if (userData != null && reply.previousCompensatorData != null) {
            userData.setLength(0);
            userData.append(reply.previousCompensatorData);
        }

        return URI.create(reply.recoveryUrl);
    }

    private String uriToString(URI uri) {
        return uri != null ? uri.toASCIIString() : null;
    }

    //    @Override
    public LRAStatus getStatus(URI lraId) {
        StatusLRAKafka.Request request = new StatusLRAKafka.Request(nextCorrelationId(), replyTopic,
                lraId.toASCIIString());
        StatusLRAKafka.Reply reply = send(LRAKafkaConstants.TYPE_STATUS, request, StatusLRAKafka.Reply.class,
                !FIRE_AND_FORGET);

        checkError(reply);
        return reply.status;
    }

    //    @Override
    public void setCurrentLRA(URI lraId) {
        // No-op for Kafka client - context is managed per-request via correlationId
    }

    //    @Override
    public void close() {
        // SmallRye Reactive Messaging handles producer/consumer lifecycle
    }
}
