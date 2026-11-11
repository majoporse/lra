package io.narayana.lra.connectors.kafka.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.LRAData;
import io.narayana.lra.connectors.common.config.CallbackConfig;
import io.narayana.lra.connectors.kafka.contracts.CancelLRAKafka;
import io.narayana.lra.connectors.kafka.contracts.CloseLRAKafka;
import io.narayana.lra.connectors.kafka.contracts.JoinLRAKafka;
import io.narayana.lra.connectors.kafka.contracts.KafkaReply;
import io.narayana.lra.connectors.kafka.contracts.LRAKafkaEnvelope;
import io.narayana.lra.connectors.kafka.contracts.LeaveLRAKafka;
import io.narayana.lra.connectors.kafka.contracts.StartLRAKafka;
import io.narayana.lra.connectors.kafka.contracts.StatusLRAKafka;
import io.narayana.lra.connectors.kafka.outbound.KafkaOutbound;
import io.narayana.lra.connectors.kafka.support.LRAKafkaConstants;
import io.narayana.lra.coordinator.config.CoordinatorConfig;
import io.narayana.lra.coordinator.domain.service.LRAService;
import io.narayana.lra.coordinator.internal.LRARecoveryModule;
import io.narayana.lra.coordinator.operations.CoordinatorOperations;
import io.narayana.lra.coordinator.operations.JoinResult;
import io.narayana.lra.coordinator.operations.StartResult;
import io.narayana.lra.logging.LRALogger;
import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
@IfBuildProperty(name = CallbackConfig.PROTOCOL_PROPERTY, stringValue = CallbackConfig.KAFKA)
public class KafkaLRAListener {

    private final LRAService lraService;
    private final CoordinatorOperations operations;
    private final ObjectMapper objectMapper;

    @Inject
    KafkaOutbound kafkaOutbound;

    @Inject
    KafkaDelivery kafkaDelivery;

    public KafkaLRAListener() {
        this.lraService = LRARecoveryModule.getService();
        this.operations = new CoordinatorOperations(lraService, new CoordinatorConfig());
        this.objectMapper = new ObjectMapper().findAndRegisterModules();
    }

    @PostConstruct
    void registerDelivery() {
        lraService.registerDelivery(kafkaDelivery);
    }

    @Incoming(LRAKafkaConstants.TOPIC_REQUEST)
    @Blocking(ordered = false)
    public CompletionStage<Void> onRequest(Message<String> message) {
        String rawJson = message.getPayload();

        try {
            LRAKafkaEnvelope envelope = objectMapper.readValue(rawJson, LRAKafkaEnvelope.class);

            if (LRAKafkaConstants.isCallbackReply(envelope.type)) {
                kafkaOutbound.completeReply(rawJson);
                return message.ack();
            }

            switch (envelope.type) {
                case StartLRAKafka.TYPE:
                    handleStart(objectMapper.convertValue(envelope.payload, StartLRAKafka.Request.class));
                    break;
                case CloseLRAKafka.TYPE:
                    handleClose(objectMapper.convertValue(envelope.payload, CloseLRAKafka.Request.class));
                    break;
                case CancelLRAKafka.TYPE:
                    handleCancel(objectMapper.convertValue(envelope.payload, CancelLRAKafka.Request.class));
                    break;
                case LeaveLRAKafka.TYPE:
                    handleLeave(objectMapper.convertValue(envelope.payload, LeaveLRAKafka.Request.class));
                    break;
                case JoinLRAKafka.TYPE:
                    handleJoin(objectMapper.convertValue(envelope.payload, JoinLRAKafka.Request.class));
                    break;
                case StatusLRAKafka.TYPE:
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
            long timeout = request.timeout == null ? 0 : request.timeout;
            StartResult result = operations.start(request.parentLRA, request.clientId, timeout);
            reply = new StartLRAKafka.Reply(request.getCorrelationId(), result.lraId(), null);
        } catch (Exception e) {
            reply = new StartLRAKafka.Reply(request.getCorrelationId(), null, e.getMessage());
        }

        sendReply(request.getReplyTopic(), reply);
    }

    private void handleClose(CloseLRAKafka.Request request) {
        CloseLRAKafka.Reply reply;

        try {
            LRAData lraData = operations.close(request.lraId, request.participantId, request.userData);
            reply = new CloseLRAKafka.Reply(request.getCorrelationId(), lraData.getStatus(), null);
        } catch (Exception e) {
            reply = new CloseLRAKafka.Reply(request.getCorrelationId(), null, e.getMessage());
        }

        sendReply(request.getReplyTopic(), reply);
    }

    private void handleCancel(CancelLRAKafka.Request request) {
        CancelLRAKafka.Reply reply;

        try {
            LRAData lraData = operations.cancel(request.lraId, request.compensator, request.userData);
            reply = new CancelLRAKafka.Reply(request.getCorrelationId(), lraData.getStatus().name(), null);
        } catch (Exception e) {
            reply = new CancelLRAKafka.Reply(request.getCorrelationId(), null, e.getMessage());
        }

        sendReply(request.getReplyTopic(), reply);
    }

    private void handleLeave(LeaveLRAKafka.Request request) {
        LeaveLRAKafka.Reply reply;

        try {
            operations.leave(request.lraId, request.participantId);
            reply = new LeaveLRAKafka.Reply(request.getCorrelationId(), null);
        } catch (Exception e) {
            reply = new LeaveLRAKafka.Reply(request.getCorrelationId(), e.getMessage());
        }

        sendReply(request.getReplyTopic(), reply);
    }

    private void handleJoin(JoinLRAKafka.Request request) {
        JoinLRAKafka.Reply reply;

        try {
            JoinResult result = operations.join(request.lraId, request.timeLimit, request.callbacks,
                    request.partId, request.userData);

            reply = new JoinLRAKafka.Reply(request.getCorrelationId(), result.recoveryUrl(),
                    result.userData(), null);
        } catch (Exception e) {
            reply = new JoinLRAKafka.Reply(request.getCorrelationId(), null, null, e.getMessage());
        }

        sendReply(request.getReplyTopic(), reply);
    }

    private void handleStatus(StatusLRAKafka.Request request) {
        StatusLRAKafka.Reply reply;

        try {
            reply = new StatusLRAKafka.Reply(request.getCorrelationId(), operations.status(request.lraId), null);
        } catch (Exception e) {
            reply = new StatusLRAKafka.Reply(request.getCorrelationId(), null, e.getMessage());
        }

        sendReply(request.getReplyTopic(), reply);
    }

    private void sendReply(String replyTopic, KafkaReply reply) {
        try {
            kafkaOutbound.sendRaw(replyTopic, reply);
        } catch (RuntimeException e) {
            LRALogger.logger.error("Failed to serialize reply", e);
        }
    }
}
