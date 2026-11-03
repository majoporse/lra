package io.narayana.lra.connectors.kafka.contracts;

import io.narayana.lra.connectors.common.contracts.CallbackOutcome;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

public interface KafkaCallbackReply extends KafkaReply, CallbackOutcome {

    String getCorrelationId();

    ParticipantStatus getStatus();

    String getError();

    @Override
    default ParticipantStatus status() {
        return getStatus();
    }

    @Override
    default String error() {
        return getError();
    }
}
