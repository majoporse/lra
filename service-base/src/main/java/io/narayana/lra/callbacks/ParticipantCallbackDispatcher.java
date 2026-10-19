package io.narayana.lra.callbacks;

import io.narayana.lra.callbacks.contracts.common.ParticipantReply;
import io.narayana.lra.callbacks.contracts.common.ParticipantRequest;

public interface ParticipantCallbackDispatcher {

    ParticipantReply dispatch(CallbackType type, ParticipantRequest request);
}
