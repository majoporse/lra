package io.narayana.lra.connectors.common.client;

import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.ParticipantRequest;

public interface ParticipantCallbackDispatcher {

    ParticipantReply dispatch(CallbackType type, ParticipantRequest request);
}
