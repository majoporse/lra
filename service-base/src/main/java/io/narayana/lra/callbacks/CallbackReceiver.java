/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.callbacks;

import io.narayana.lra.callbacks.contracts.common.ParticipantReply;
import io.narayana.lra.callbacks.contracts.common.ParticipantRequest;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

/**
 * The one participant-side callback handler. Inbound adapters (HTTP resources, the Kafka listener) turn
 * a wire message into a {@link ParticipantRequest} and call {@link #receive(CallbackType, ParticipantRequest)}.
 * <p>
 * Transport-neutral: it knows nothing about HTTP, Kafka, envelopes or topics. It validates the request,
 * finds the participant through {@link ParticipantCallbackDispatcher}, invokes it and turns every failure
 * into the common error reply.
 */
@ApplicationScoped
public class CallbackReceiver {

    @Inject
    Instance<ParticipantCallbackDispatcher> participantCallbackDispatcher;

    private final ParticipantCallbackDispatcher dispatcher;

    public CallbackReceiver() {
        this.dispatcher = null;
    }

    /**
     * Test seam: construct a receiver with an explicit dispatcher.
     */
    public CallbackReceiver(ParticipantCallbackDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public ParticipantReply receive(CallbackType type, ParticipantRequest request) {
        if (request == null) {
            return errorReply("Missing " + type + " callback request");
        }

        if (request.participantId == null || request.participantId.isBlank()) {
            return errorReply("Missing participant id for " + type + " callback");
        }

        ParticipantCallbackDispatcher target = dispatcher();

        if (target == null) {
            return errorReply(
                    LRALogger.i18nLogger.error_missingCallbackParticipant(request.participantId, type.name()));
        }

        try {
            ParticipantReply reply = target.dispatch(type, request);

            return reply != null ? reply
                    : errorReply(
                            LRALogger.i18nLogger.error_missingCallbackParticipant(request.participantId,
                                    type.name()));
        } catch (RuntimeException e) {
            LRALogger.logger.errorf("LRA %s callback %s failed for participant '%s': %s",
                    request.lraId, type, request.participantId,
                    e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());

            return errorReply(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
    }

    private ParticipantCallbackDispatcher dispatcher() {
        if (dispatcher != null) {
            return dispatcher;
        }

        return participantCallbackDispatcher != null && participantCallbackDispatcher.isResolvable()
                ? participantCallbackDispatcher.get()
                : null;
    }

    private static ParticipantReply errorReply(String message) {
        ParticipantReply reply = new ParticipantReply();
        reply.error = message;
        return reply;
    }
}
