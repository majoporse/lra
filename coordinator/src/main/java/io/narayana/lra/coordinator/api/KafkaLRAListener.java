package io.narayana.lra.coordinator.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.LRAData;
import io.narayana.lra.contracts.kafka.CancelLRA;
import io.narayana.lra.contracts.kafka.CloseLRA;
import io.narayana.lra.contracts.kafka.JoinLRA;
import io.narayana.lra.contracts.kafka.LRAKafkaConstants;
import io.narayana.lra.contracts.kafka.LeaveLRA;
import io.narayana.lra.contracts.kafka.StartLRA;
import io.narayana.lra.contracts.kafka.StatusLRA;
import io.narayana.lra.coordinator.domain.model.LongRunningAction;
import io.narayana.lra.coordinator.domain.service.LRAService;
import io.narayana.lra.coordinator.internal.LRARecoveryModule;
import io.narayana.lra.logging.LRALogger;
import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
@IfBuildProperty(name = "lra.kafka.enabled", stringValue = "true")
public class KafkaLRAListener {

    private final LRAService lraService;
    private final ObjectMapper objectMapper;

    @Channel(LRAKafkaConstants.TOPIC_REPLY)
    Emitter<String> replyEmitter;

    public KafkaLRAListener() {
        this.lraService = LRARecoveryModule.getService();
        this.objectMapper = new ObjectMapper();
    }

    @Incoming(LRAKafkaConstants.TOPIC_START)
    public CompletionStage<Void> onStartLRA(Message<StartLRA.Request> message) {
        StartLRA.Request request = message.getPayload();
        StartLRA.Reply reply;

        try {
            String coordinatorUrl = "http://localhost:8080/" + LRAConstants.COORDINATOR_PATH_NAME;
            URI parentId = request.parentLRA != null && !request.parentLRA.isEmpty()
                    ? URI.create(request.parentLRA)
                    : null;
            LongRunningAction lra = lraService.startLRA(coordinatorUrl, parentId, request.clientId, request.timeout);
            reply = new StartLRA.Reply(request.correlationId, lra.getId().toASCIIString(), null);
        } catch (Exception e) {
            reply = new StartLRA.Reply(request.correlationId, null, e.getMessage());
        }

        sendReply(request.replyTopic, reply);
        return message.ack();
    }

    @Incoming(LRAKafkaConstants.TOPIC_CLOSE)
    public CompletionStage<Void> onCloseLRA(Message<CloseLRA.Request> message) {
        CloseLRA.Request request = message.getPayload();
        CloseLRA.Reply reply;

        try {
            URI lraId = URI.create(request.lraId);
            LRAData lraData = lraService.endLRA(lraId, false, false, request.compensator, request.userData);
            reply = new CloseLRA.Reply(request.correlationId, lraData.getStatus().name(), null);
        } catch (Exception e) {
            reply = new CloseLRA.Reply(request.correlationId, null, e.getMessage());
        }

        sendReply(request.replyTopic, reply);
        return message.ack();
    }

    @Incoming(LRAKafkaConstants.TOPIC_CANCEL)
    public CompletionStage<Void> onCancelLRA(Message<CancelLRA.Request> message) {
        CancelLRA.Request request = message.getPayload();
        CancelLRA.Reply reply;

        try {
            URI lraId = URI.create(request.lraId);
            LRAData lraData = lraService.endLRA(lraId, true, false, request.compensator, request.userData);
            reply = new CancelLRA.Reply(request.correlationId, lraData.getStatus().name(), null);
        } catch (Exception e) {
            reply = new CancelLRA.Reply(request.correlationId, null, e.getMessage());
        }

        sendReply(request.replyTopic, reply);
        return message.ack();
    }

    @Incoming(LRAKafkaConstants.TOPIC_LEAVE)
    public CompletionStage<Void> onLeaveLRA(Message<LeaveLRA.Request> message) {
        LeaveLRA.Request request = message.getPayload();
        LeaveLRA.Reply reply;

        try {
            URI lraId = URI.create(request.lraId);
            lraService.leave(lraId, request.body);
            reply = new LeaveLRA.Reply(request.correlationId, null);
        } catch (Exception e) {
            reply = new LeaveLRA.Reply(request.correlationId, e.getMessage());
        }

        sendReply(request.replyTopic, reply);
        return message.ack();
    }

    @Incoming(LRAKafkaConstants.TOPIC_JOIN)
    public CompletionStage<Void> onJoinLRA(Message<JoinLRA.Request> message) {
        JoinLRA.Request request = message.getPayload();
        JoinLRA.Reply reply;

        try {
            URI lraId = URI.create(request.lraId);
            String recoveryUrlBase = "http://localhost:8080/" + LRAConstants.COORDINATOR_PATH_NAME + "/"
                    + LRAConstants.RECOVERY_COORDINATOR_PATH_NAME;
            StringBuilder recoveryUrl = new StringBuilder();
            StringBuilder compensatorData = request.compensatorData != null
                    ? new StringBuilder(request.compensatorData)
                    : new StringBuilder();

            String linkHeader = buildLinkHeader(request);

            int status = lraService.joinLRA(recoveryUrl, lraId, request.timeLimit, null,
                    linkHeader, recoveryUrlBase, compensatorData);

            if (status == Response.Status.OK.getStatusCode()) {
                reply = new JoinLRA.Reply(request.correlationId, recoveryUrl.toString(),
                        compensatorData.toString(), null);
            } else {
                reply = new JoinLRA.Reply(request.correlationId, null, null,
                        "Join failed with status: " + status);
            }
        } catch (Exception e) {
            reply = new JoinLRA.Reply(request.correlationId, null, null, e.getMessage());
        }

        sendReply(request.replyTopic, reply);
        return message.ack();
    }

    private String buildLinkHeader(JoinLRA.Request request) {
        StringBuilder sb = new StringBuilder();
        appendLink(sb, "compensate", request.compensateLink);
        appendLink(sb, "complete", request.completeLink);
        appendLink(sb, "forget", request.forgetLink);
        appendLink(sb, "leave", request.leaveLink);
        appendLink(sb, "after", request.afterLink);
        appendLink(sb, "status", request.statusLink);
        return sb.toString();
    }

    private void appendLink(StringBuilder sb, String rel, String url) {
        if (url != null && !url.isEmpty()) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append("<").append(url).append(">;rel=\"").append(rel).append("\"");
        }
    }

    @Incoming(LRAKafkaConstants.TOPIC_STATUS)
    public CompletionStage<Void> onStatusLRA(Message<StatusLRA.Request> message) {
        StatusLRA.Request request = message.getPayload();
        StatusLRA.Reply reply;

        try {
            URI lraId = URI.create(request.lraId);
            LongRunningAction lra = lraService.getTransaction(lraId);
            LRAStatus status = lra.getLRAStatus();
            if (status == null) {
                status = LRAStatus.Active;
            }
            reply = new StatusLRA.Reply(request.correlationId, status.name(), null);
        } catch (Exception e) {
            reply = new StatusLRA.Reply(request.correlationId, null, e.getMessage());
        }

        sendReply(request.replyTopic, reply);
        return message.ack();
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
}
