package io.narayana.lra.callbacks.contracts.common;

import io.narayana.lra.callbacks.CallbackResult;

/**
 * Transport-agnostic base for the reply to a participant notification. The
 * result of a @Compensate/@Complete/@Status invocation.
 */
public class ParticipantReply {
    public CallbackResult result;

    public ParticipantReply() {
    }

    protected ParticipantReply(CallbackResult result) {
        this.result = result;
    }
}
