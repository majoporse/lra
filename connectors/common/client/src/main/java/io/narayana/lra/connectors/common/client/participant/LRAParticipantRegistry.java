package io.narayana.lra.connectors.common.client.participant;

import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.client.ParticipantCallbackDispatcher;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.ParticipantRequest;
import io.narayana.lra.proxy.logging.LRAProxyLogger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The one participant registry: it holds both participant forms, keyed by participant id (the
 * participant class name), and dispatches a callback to the model that declares it.
 * <p>
 * The registry is registered as a bean in both participant modes; it is empty when the active mode
 * does not collect any participant.
 */
public class LRAParticipantRegistry implements ParticipantCallbackDispatcher {

    private final Map<String, Participant> lraParticipants;

    // required for Weld to be able to create proxy
    public LRAParticipantRegistry() {
        this(Collections.emptyMap());
    }

    public LRAParticipantRegistry(Map<String, Participant> lraParticipants) {
        this.lraParticipants = new HashMap<>(lraParticipants);
    }

    public Participant getParticipant(String id) {
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

        Participant participant = getParticipant(request.participantId);

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
