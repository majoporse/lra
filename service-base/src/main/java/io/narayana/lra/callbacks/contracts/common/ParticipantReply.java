package io.narayana.lra.callbacks.contracts.common;

import io.narayana.lra.api.LRA;

public class ParticipantReply {
    public LRA.ParticipantOutcome status;
    public String error;

    public ParticipantReply() {
    }

    protected ParticipantReply(LRA.ParticipantOutcome status) {
        this(status, null);
    }

    protected ParticipantReply(LRA.ParticipantOutcome status, String error) {
        this.status = status;
        this.error = error;
    }
}
