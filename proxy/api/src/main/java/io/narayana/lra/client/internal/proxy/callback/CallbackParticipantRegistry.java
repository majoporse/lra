package io.narayana.lra.client.internal.proxy.callback;

import io.narayana.lra.callbacks.CallbackType;
import io.narayana.lra.callbacks.ParticipantCallbackDispatcher;
import io.narayana.lra.callbacks.contracts.common.ParticipantReply;
import io.narayana.lra.callbacks.contracts.common.ParticipantRequest;
import io.narayana.lra.proxy.logging.LRAProxyLogger;
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
        if (request == null) {
            return errorReply("Missing callback request");
        }

        if (request.participantId == null || request.participantId.isBlank()) {
            return errorReply("Missing participant id for " + type + " callback");
        }

        ContractParticipant participant = getParticipant(request.participantId);

        if (participant == null) {
            return errorReply(LRAProxyLogger.i18NLogger.error_missingParticipant(request.participantId));
        }

        ParticipantReply reply = participant.invoke(type, request);

        return reply != null ? reply
                : errorReply(LRAProxyLogger.i18NLogger.error_missingCallbackMethod(request.participantId,
                        type.name()));
    }

    private static ParticipantReply errorReply(String message) {
        ParticipantReply reply = new ParticipantReply();
        reply.error = message;
        return reply;
    }
}
