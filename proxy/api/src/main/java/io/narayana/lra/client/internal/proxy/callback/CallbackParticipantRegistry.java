package io.narayana.lra.client.internal.proxy.callback;

import io.narayana.lra.callbacks.CallbackType;
import io.narayana.lra.callbacks.ParticipantCallbackDispatcher;
import io.narayana.lra.callbacks.contracts.common.ParticipantReply;
import io.narayana.lra.callbacks.contracts.common.ParticipantRequest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CallbackParticipantRegistry implements ParticipantCallbackDispatcher {

    private final Map<String, ContractParticipant> lraParticipants;

    // required for Weld to be able to create proxy
    public CallbackParticipantRegistry() {
        this(Collections.emptyMap());
    }

    public CallbackParticipantRegistry(Map<String, ContractParticipant> lraParticipants) {
        this.lraParticipants = new HashMap<>(lraParticipants);
    }

    public ContractParticipant getParticipant(String id) {
        return lraParticipants.get(id);
    }

    @Override
    public ParticipantReply dispatch(CallbackType type, ParticipantRequest request) {
        if (request == null || request.participantId == null) {
            return null;
        }

        ContractParticipant participant = getParticipant(request.participantId);

        return participant == null ? null : participant.invoke(type, request);
    }
}
