package io.narayana.lra.coordinator.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.LRAData;
import io.narayana.lra.contracts.kafka.CancelLRAKafka;
import io.narayana.lra.contracts.kafka.CloseLRAKafka;
import io.narayana.lra.contracts.kafka.JoinLRAKafka;
import io.narayana.lra.contracts.kafka.LRAKafkaConstants;
import io.narayana.lra.contracts.kafka.LRAKafkaEnvelope;
import io.narayana.lra.contracts.kafka.LeaveLRAKafka;
import io.narayana.lra.contracts.kafka.StartLRAKafka;
import io.narayana.lra.contracts.kafka.StatusLRAKafka;
import io.narayana.lra.coordinator.domain.model.LongRunningAction;
import io.narayana.lra.coordinator.domain.service.HttpLRAService;
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
@IfBuildProperty(name = "quarkus.lra.kafka.enabled", stringValue = "true")
public class KafkaLRAListener {

    private final LRAService lraService;
    private final HttpLRAService httpLraService;
    private final ObjectMapper objectMapper;

    @Channel(LRAKafkaConstants.TOPIC_REPLY)
    Emitter<String> replyEmitter;

    public KafkaLRAListener() {
        this.lraService = LRARecoveryModule.getService();
        this.httpLraService = LRARecoveryModule.getHttpService();
        this.objectMapper = new ObjectMapper();
    }

    @Incoming(LRAKafkaConstants.TOPIC_REQUEST)
    public CompletionStage<Void> onRequest(Message<String> message) {
        String rawJson = message.getPayload();

        try {
            LRAKafkaEnvelope envelope = objectMapper.readValue(rawJson, LRAKafkaEnvelope.class);

            switch (envelope.type) {
                case LRAKafkaConstants.TYPE_START:
                    handleStart(objectMapper.convertValue(envelope.payload, StartLRAKafka.Request.class));
                    break;
                case LRAKafkaConstants.TYPE_CLOSE:
                    handleClose(objectMapper.convertValue(envelope.payload, CloseLRAKafka.Request.class));
                    break;
                case LRAKafkaConstants.TYPE_CANCEL:
                    handleCancel(objectMapper.convertValue(envelope.payload, CancelLRAKafka.Request.class));
                    break;
                case LRAKafkaConstants.TYPE_LEAVE:
                    handleLeave(objectMapper.convertValue(envelope.payload, LeaveLRAKafka.Request.class));
                    break;
                case LRAKafkaConstants.TYPE_JOIN:
                    handleJoin(objectMapper.convertValue(envelope.payload, JoinLRAKafka.Request.class));
                    break;
                case LRAKafkaConstants.TYPE_STATUS:
                    handleStatus(objectMapper.convertValue(envelope.payload, StatusLRAKafka.Request.class));
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

    private void handleStart(StartLRAKafka.Request request) {
        StartLRAKafka.Reply reply;

        try {
            String coordinatorUrl = "http://localhost:8080/" + LRAConstants.COORDINATOR_PATH_NAME;
            URI parentId = request.parentLRA;
            LongRunningAction lra = httpLraService.startLRA(coordinatorUrl, parentId, request.clientId, request.timeout);
            reply = new StartLRAKafka.Reply(request.getCorrelationId(), HttpLRAService.toURI(lra), null);
        } catch (Exception e) {
            reply = new StartLRAKafka.Reply(request.getCorrelationId(), null, e.getMessage());
        }

        sendReply(request.getReplyTopic(), reply);
    }

    private void handleClose(CloseLRAKafka.Request request) {
        CloseLRAKafka.Reply reply;

        try {
            URI lraId = request.lraId;
            LRAData lraData = httpLraService.endLRA(lraId, false, false, request.participantId, request.userData);
            reply = new CloseLRAKafka.Reply(request.getCorrelationId(), lraData.getStatus(), null);
        } catch (Exception e) {
            reply = new CloseLRAKafka.Reply(request.getCorrelationId(), null, e.getMessage());
        }

        sendReply(request.getReplyTopic(), reply);
    }

    private void handleCancel(CancelLRAKafka.Request request) {
        CancelLRAKafka.Reply reply;

        try {
            URI lraId = request.lraId;
            LRAData lraData = httpLraService.endLRA(lraId, true, false, request.compensator, request.userData);
            reply = new CancelLRAKafka.Reply(request.getCorrelationId(), lraData.getStatus().name(), null);
        } catch (Exception e) {
            reply = new CancelLRAKafka.Reply(request.getCorrelationId(), null, e.getMessage());
        }

        sendReply(request.getReplyTopic(), reply);
    }

    private void handleLeave(LeaveLRAKafka.Request request) {
        LeaveLRAKafka.Reply reply;

        try {
            URI lraId = request.lraId;
            //            httpLraService.leave(lraId, equest.body);
            reply = new LeaveLRAKafka.Reply(request.getCorrelationId(), null);
        } catch (Exception e) {
            reply = new LeaveLRAKafka.Reply(request.getCorrelationId(), e.getMessage());
        }

        sendReply(request.getReplyTopic(), reply);
    }

    private void handleJoin(JoinLRAKafka.Request request) {
        JoinLRAKafka.Reply reply;

        try {
            URI lraId = request.lraId;
            String recoveryUrlBase = "http://localhost:8080/" + LRAConstants.COORDINATOR_PATH_NAME + "/"
                    + LRAConstants.RECOVERY_COORDINATOR_PATH_NAME;
            StringBuilder recoveryUrl = new StringBuilder();
            StringBuilder compensatorData = new StringBuilder(request.userData);

            String linkHeader = buildLinkHeader(request);
            String partId = "";

            int status = httpLraService.joinLRA(recoveryUrl, lraId, request.timeLimit, null,
                    linkHeader, recoveryUrlBase, compensatorData, partId);

            if (status == Response.Status.OK.getStatusCode()) {
                reply = new JoinLRAKafka.Reply(request.getCorrelationId(), recoveryUrl.toString(),
                        compensatorData.toString(), null);
            } else {
                reply = new JoinLRAKafka.Reply(request.getCorrelationId(), null, null,
                        "Join failed with status: " + status);
            }
        } catch (Exception e) {
            reply = new JoinLRAKafka.Reply(request.getCorrelationId(), null, null, e.getMessage());
        }

        sendReply(request.getReplyTopic(), reply);
    }

    private void handleStatus(StatusLRAKafka.Request request) {
        StatusLRAKafka.Reply reply;

        try {
            URI lraId = URI.create(request.lraId);
            LongRunningAction lra = httpLraService.getTransaction(lraId);
            LRAStatus status = lra.getLRAStatus();
            if (status == null) {
                status = LRAStatus.Active;
            }
            reply = new StatusLRAKafka.Reply(request.getCorrelationId(), status, null);
        } catch (Exception e) {
            reply = new StatusLRAKafka.Reply(request.getCorrelationId(), null, e.getMessage());
        }

        sendReply(request.getReplyTopic(), reply);
    }

    private String buildLinkHeader(JoinLRAKafka.Request request) {
        StringBuilder sb = new StringBuilder();
        appendLink(sb, "compensate", request.links.compensateLink);
        appendLink(sb, "complete", request.links.completeLink);
        appendLink(sb, "forget", request.links.forgetLink);
        appendLink(sb, "leave", request.links.leaveLink);
        appendLink(sb, "after", request.links.afterLink);
        appendLink(sb, "status", request.links.statusLink);
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
