package io.narayana.lra.connectors.common.contracts;

import io.narayana.lra.api.LRA;

public class ParticipantReply implements CallbackOutcome {
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

    @Override
    public LRA.ParticipantOutcome status() {
        return status;
    }

    @Override
    public String error() {
        return error;
    }
}
