package io.narayana.lra.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.contracts.kafka.CancelLRA;
import io.narayana.lra.contracts.kafka.CloseLRA;
import io.narayana.lra.contracts.kafka.JoinLRA;
import io.narayana.lra.contracts.kafka.LRAKafkaConstants;
import io.narayana.lra.contracts.kafka.LRAKafkaReply;
import io.narayana.lra.contracts.kafka.LRAKafkaRequest;
import io.narayana.lra.contracts.kafka.LeaveLRA;
import io.narayana.lra.contracts.kafka.StartLRA;
import io.narayana.lra.contracts.kafka.StatusLRA;
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

    @Inject
    @Channel(LRAKafkaConstants.TOPIC_START)
    Emitter<StartLRA.Request> startEmitter;

    @Inject
    @Channel(LRAKafkaConstants.TOPIC_CLOSE)
    Emitter<CloseLRA.Request> closeEmitter;

    @Inject
    @Channel(LRAKafkaConstants.TOPIC_CANCEL)
    Emitter<CancelLRA.Request> cancelEmitter;

    @Inject
    @Channel(LRAKafkaConstants.TOPIC_LEAVE)
    Emitter<LeaveLRA.Request> leaveEmitter;

    @Inject
    @Channel(LRAKafkaConstants.TOPIC_JOIN)
    Emitter<JoinLRA.Request> joinEmitter;

    @Inject
    @Channel(LRAKafkaConstants.TOPIC_STATUS)
    Emitter<StatusLRA.Request> statusEmitter;

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

    private <T extends LRAKafkaRequest, R extends LRAKafkaReply> R sendAndWait(
            Emitter<T> emitter, T request, Class<R> replyClass) {
        String correlationId = request.correlationId;
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

    @Override
    public URI startLRA(URI parentLRA, String clientID, Long timeout, ChronoUnit unit, boolean verbose) {
        String correlationId = UUID.randomUUID().toString();
        Long timeoutMillis = timeout != null ? java.time.Duration.of(timeout, unit).toMillis() : 0L;
        String parentStr = parentLRA != null ? parentLRA.toASCIIString() : null;

        StartLRA.Request request = new StartLRA.Request(correlationId, replyTopic, clientID, timeoutMillis, parentStr);
        StartLRA.Reply reply = sendAndWait(startEmitter, request, StartLRA.Reply.class);

        if (reply.error != null) {
            throw new WebApplicationException(reply.error, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        return URI.create(reply.lraId);
    }

    @Override
    public void closeLRA(URI lraId, String compensator, String userData) {
        String correlationId = UUID.randomUUID().toString();
        CloseLRA.Request request = new CloseLRA.Request(correlationId, replyTopic, lraId.toASCIIString(), compensator,
                userData);
        CloseLRA.Reply reply = sendAndWait(closeEmitter, request, CloseLRA.Reply.class);

        if (reply.error != null) {
            throw new WebApplicationException(reply.error, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @Override
    public void cancelLRA(URI lraId, String compensator, String userData) {
        String correlationId = UUID.randomUUID().toString();
        CancelLRA.Request request = new CancelLRA.Request(correlationId, replyTopic, lraId.toASCIIString(), compensator,
                userData);
        CancelLRA.Reply reply = sendAndWait(cancelEmitter, request, CancelLRA.Reply.class);

        if (reply.error != null) {
            throw new WebApplicationException(reply.error, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @Override
    public void leaveLRA(URI lraId, String body) {
        String correlationId = UUID.randomUUID().toString();
        LeaveLRA.Request request = new LeaveLRA.Request(correlationId, replyTopic, lraId.toASCIIString(), body);
        LeaveLRA.Reply reply = sendAndWait(leaveEmitter, request, LeaveLRA.Reply.class);

        if (reply.error != null) {
            throw new WebApplicationException(reply.error, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @Override
    public URI joinLRA(URI lraId, Long timeLimit,
            URI compensateUri, URI completeUri,
            URI forgetUri, URI leaveUri, URI afterUri, URI statusUri,
            StringBuilder compensatorData) {
        String correlationId = UUID.randomUUID().toString();
        String data = compensatorData != null ? compensatorData.toString() : null;

        JoinLRA.Request request = new JoinLRA.Request(
                correlationId, replyTopic, lraId.toASCIIString(), timeLimit,
                uriToString(compensateUri), uriToString(completeUri), uriToString(forgetUri),
                uriToString(leaveUri), uriToString(afterUri), uriToString(statusUri),
                data);

        JoinLRA.Reply reply = sendAndWait(joinEmitter, request, JoinLRA.Reply.class);

        if (reply.error != null) {
            throw new WebApplicationException(reply.error, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

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
        String correlationId = UUID.randomUUID().toString();
        StatusLRA.Request request = new StatusLRA.Request(correlationId, replyTopic, lraId.toASCIIString());
        StatusLRA.Reply reply = sendAndWait(statusEmitter, request, StatusLRA.Reply.class);

        if (reply.error != null) {
            throw new WebApplicationException(reply.error, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
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
