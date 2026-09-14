package io.narayana.lra.callbacks.contracts.common;

import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

/**
 * Transport-agnostic base for the reply to a participant notification. The
 * reported progress of a @Compensate/@Complete/@Status invocation.
 */
public class ParticipantReply {
    public ParticipantStatus status;

    public ParticipantReply() {
    }

    protected ParticipantReply(ParticipantStatus status) {
        this.status = status;
    }
}
