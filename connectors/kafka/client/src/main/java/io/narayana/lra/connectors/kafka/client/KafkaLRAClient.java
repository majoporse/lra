package io.narayana.lra.connectors.kafka.client;

import io.narayana.lra.LRAConstants;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.CallbackPaths;
import io.narayana.lra.connectors.common.client.LRAClient;
import io.narayana.lra.connectors.common.client.ParticipantConnector;
import io.narayana.lra.connectors.common.config.CallbackConfig;
import io.narayana.lra.connectors.kafka.contracts.CancelLRAKafka;
import io.narayana.lra.connectors.kafka.contracts.CloseLRAKafka;
import io.narayana.lra.connectors.kafka.contracts.JoinLRAKafka;
import io.narayana.lra.connectors.kafka.contracts.KafkaReply;
import io.narayana.lra.connectors.kafka.contracts.KafkaRequest;
import io.narayana.lra.connectors.kafka.contracts.LeaveLRAKafka;
import io.narayana.lra.connectors.kafka.contracts.StartLRAKafka;
import io.narayana.lra.connectors.kafka.contracts.StatusLRAKafka;
import io.narayana.lra.connectors.kafka.outbound.KafkaOutbound;
import io.narayana.lra.connectors.kafka.support.LRAKafkaConstants;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

@ApplicationScoped
@IfBuildProperty(name = CallbackConfig.PROTOCOL_PROPERTY, stringValue = CallbackConfig.KAFKA)
@ParticipantConnector(CallbackPaths.KAFKA)
public class KafkaLRAClient implements LRAClient {
    private static final long REPLY_TIMEOUT_SECONDS = 30;
    private static final boolean FIRE_AND_FORGET = true;

    @Inject
    KafkaOutbound kafkaOutbound;

    private final String replyTopic;

    public KafkaLRAClient() {
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

    private String coordinatorTopic() {
        return kafkaOutbound.coordinatorTopic();
    }

    private <R extends KafkaReply, T extends KafkaRequest<R>> R send(T request, boolean fireAndForget) {
        if (fireAndForget) {
            kafkaOutbound.send(coordinatorTopic(), request);
            return null;
        }

        R reply;

        try {
            reply = kafkaOutbound.request(coordinatorTopic(), request,
                    Duration.ofSeconds(REPLY_TIMEOUT_SECONDS));
        } catch (RuntimeException e) {
            throw new WebApplicationException("Kafka request failed: " + e.getMessage(),
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        if (reply == null) {
            throw new WebApplicationException("Kafka request timed out",
                    Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
        }

        return reply;
    }

    private String nextCorrelationId() {
        return UUID.randomUUID().toString();
    }

    private void checkError(KafkaReply reply) {
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
                parentLRA == null ? null : LRAConstants.toUuid(parentLRA));
        StartLRAKafka.Reply reply = send(request, false);

        checkError(reply);
        return LRAConstants.toLRAId(reply.lraId);
    }

    //    @Override
    public void closeLRA(URI lraId, String compensator, String userData) {
        CloseLRAKafka.Request request = new CloseLRAKafka.Request(nextCorrelationId(), replyTopic,
                LRAConstants.toUuid(lraId), compensator, userData);
        send(request, FIRE_AND_FORGET);
    }

    //    @Override
    public void cancelLRA(URI lraId, String compensator, String userData) {
        CancelLRAKafka.Request request = new CancelLRAKafka.Request(nextCorrelationId(), replyTopic,
                LRAConstants.toUuid(lraId), compensator, userData);
        send(request, FIRE_AND_FORGET);
    }

    //    @Override
    public void leaveLRA(URI lraId, String body) {
        LeaveLRAKafka.Request request = new LeaveLRAKafka.Request(nextCorrelationId(), replyTopic,
                LRAConstants.toUuid(lraId), body);
        send(request, FIRE_AND_FORGET);
    }

    @Override
    public String protocolId() {
        return CallbackPaths.KAFKA;
    }

    @Override
    public URI enlistCompensator(URI lraId, Long timeLimit, ParticipantCallbacks callbacks,
            StringBuilder previousParticipantData, String participantId) {
        return joinLRA(lraId, timeLimit, callbacks, previousParticipantData, participantId);
    }

    //    @Override
    public URI joinLRA(URI lraId, Long timeLimit, ParticipantCallbacks callbacks,
            StringBuilder userData, String partId) {

        JoinLRAKafka.Request request = new JoinLRAKafka.Request(
                nextCorrelationId(), replyTopic, LRAConstants.toUuid(lraId), timeLimit, callbacks, userData.toString(), partId);

        JoinLRAKafka.Reply reply = send(request, false);

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
                LRAConstants.toUuid(lraId));
        StatusLRAKafka.Reply reply = send(request, !FIRE_AND_FORGET);

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
