package io.narayana.lra.connectors.kafka.contracts;

import io.narayana.lra.api.LRA;
import io.narayana.lra.connectors.common.contracts.CallbackOutcome;

public interface KafkaCallbackReply extends KafkaReply, CallbackOutcome {

    String getCorrelationId();

    LRA.ParticipantOutcome getStatus();

    String getError();

    @Override
    default LRA.ParticipantOutcome status() {
        return getStatus();
    }

    @Override
    default String error() {
        return getError();
    }
}
