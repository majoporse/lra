package io.narayana.lra.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.contracts.kafka.CancelLRAKafka;
import io.narayana.lra.contracts.kafka.CloseLRAKafka;
import io.narayana.lra.contracts.kafka.JoinLRAKafka;
import io.narayana.lra.contracts.kafka.LRAKafkaConstants;
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
public class KafkaLRAClient implements LRAClient {
    private static final long REPLY_TIMEOUT_SECONDS = 30;
    private static final boolean FIRE_AND_FORGET = true;

    @Inject
    @Channel(LRAKafkaConstants.TOPIC_START)
    Emitter<StartLRAKafka.Request> startEmitter;

    @Inject
    @Channel(LRAKafkaConstants.TOPIC_CLOSE)
    Emitter<CloseLRAKafka.Request> closeEmitter;

    @Inject
    @Channel(LRAKafkaConstants.TOPIC_CANCEL)
    Emitter<CancelLRAKafka.Request> cancelEmitter;

    @Inject
    @Channel(LRAKafkaConstants.TOPIC_LEAVE)
    Emitter<LeaveLRAKafka.Request> leaveEmitter;

    @Inject
    @Channel(LRAKafkaConstants.TOPIC_JOIN)
    Emitter<JoinLRAKafka.Request> joinEmitter;

    @Inject
    @Channel(LRAKafkaConstants.TOPIC_STATUS)
    Emitter<StatusLRAKafka.Request> statusEmitter;

    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<String, CompletableFuture<String>> pendingReplies;
    private final String replyTopic;

    public KafkaLRAClient() {
        this.objectMapper = new ObjectMapper();
        this.pendingReplies = new ConcurrentHashMap<>();
        this.replyTopic = resolveReplyTopic();
    }

    private String resolveReplyTopic() {
        String serviceId = ConfigProvider.getConfig()
                .getOptionalValue(LRAKafkaConstants.CONFIG_SERVICE_ID, String.class)
                .orElse(null);
        if (serviceId == null || serviceId.isEmpty()) {
            throw new IllegalStateException(
                    "Missing required config property '" + LRAKafkaConstants.CONFIG_SERVICE_ID
                            + "'. Each service must have a unique ID for Kafka reply routing.");
        }
        return LRAKafkaConstants.TOPIC_REPLY_PREFIX + serviceId;
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
            Emitter<T> emitter, T request, Class<R> replyClass, boolean fireAndForget) {
        if (fireAndForget) {
            sendAndForget(emitter, request);
            return null;
        }
        return sendAndWait(emitter, request, replyClass);
    }

    private <T extends LRAKafkaRequest> void sendAndForget(Emitter<T> emitter, T request) {
        emitter.send(request);
    }

    private <T extends LRAKafkaRequest, R extends LRAKafkaReply> R sendAndWait(
            Emitter<T> emitter, T request, Class<R> replyClass) {
        String correlationId = request.getCorrelationId();
        CompletableFuture<String> future = new CompletableFuture<>();
        pendingReplies.put(correlationId, future);

        emitter.send(request);

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
        if (reply == null || reply.getError() != null) {
            throw new WebApplicationException(reply.getError(),
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @Override
    public URI startLRA(URI parentLRA, String clientID, Long timeout, ChronoUnit unit, boolean verbose) {
        Long timeoutMillis = timeout != null ? java.time.Duration.of(timeout, unit).toMillis() : 0L;
        String parentStr = parentLRA != null ? parentLRA.toASCIIString() : null;

        StartLRAKafka.Request request = new StartLRAKafka.Request(nextCorrelationId(), replyTopic, clientID, timeoutMillis, parentStr);
        StartLRAKafka.Reply reply = send(startEmitter, request, StartLRAKafka.Reply.class, false);

        checkError(reply);
        return URI.create(reply.lraId);
    }

    @Override
    public void closeLRA(URI lraId, String compensator, String userData) {
        CloseLRAKafka.Request request = new CloseLRAKafka.Request(nextCorrelationId(), replyTopic,
                lraId.toASCIIString(), compensator, userData);
        send(closeEmitter, request, CloseLRAKafka.Reply.class, FIRE_AND_FORGET);
    }

    @Override
    public void cancelLRA(URI lraId, String compensator, String userData) {
        CancelLRAKafka.Request request = new CancelLRAKafka.Request(nextCorrelationId(), replyTopic,
                lraId.toASCIIString(), compensator, userData);
        send(cancelEmitter, request, CancelLRAKafka.Reply.class, FIRE_AND_FORGET);
    }

    @Override
    public void leaveLRA(URI lraId, String body) {
        LeaveLRAKafka.Request request = new LeaveLRAKafka.Request(nextCorrelationId(), replyTopic,
                lraId.toASCIIString(), body);
        send(leaveEmitter, request, LeaveLRAKafka.Reply.class, FIRE_AND_FORGET);
    }

    @Override
    public URI joinLRA(URI lraId, Long timeLimit,
            URI compensateUri, URI completeUri,
            URI forgetUri, URI leaveUri, URI afterUri, URI statusUri,
            StringBuilder compensatorData) {
        String data = compensatorData != null ? compensatorData.toString() : null;

        JoinLRAKafka.Request request = new JoinLRAKafka.Request(
                nextCorrelationId(), replyTopic, lraId.toASCIIString(), timeLimit,
                uriToString(compensateUri), uriToString(completeUri), uriToString(forgetUri),
                uriToString(leaveUri), uriToString(afterUri), uriToString(statusUri),
                data);

        JoinLRAKafka.Reply reply = send(joinEmitter, request, JoinLRAKafka.Reply.class, false);

        checkError(reply);

        if (compensatorData != null && reply.previousCompensatorData != null) {
            compensatorData.setLength(0);
            compensatorData.append(reply.previousCompensatorData);
        }

        return URI.create(reply.recoveryUrl);
    }

    private String uriToString(URI uri) {
        return uri != null ? uri.toASCIIString() : null;
    }

    @Override
    public LRAStatus getStatus(URI lraId) {
        StatusLRAKafka.Request request = new StatusLRAKafka.Request(nextCorrelationId(), replyTopic, lraId.toASCIIString());
        StatusLRAKafka.Reply reply = send(statusEmitter, request, StatusLRAKafka.Reply.class, false);

        checkError(reply);
        return LRAStatus.valueOf(reply.status);
    }

    @Override
    public void setCurrentLRA(URI lraId) {
        // No-op for Kafka client - context is managed per-request via correlationId
    }

    @Override
    public void close() {
        // SmallRye Reactive Messaging handles producer/consumer lifecycle
    }
}
