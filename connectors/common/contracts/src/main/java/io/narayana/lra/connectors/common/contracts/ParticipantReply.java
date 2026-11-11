package io.narayana.lra.connectors.common.contracts;

import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

public class ParticipantReply implements CallbackOutcome {
    public ParticipantStatus status;
    public String error;

    public ParticipantReply() {
    }

    protected ParticipantReply(ParticipantStatus status) {
        this(status, null);
    }

    protected ParticipantReply(ParticipantStatus status, String error) {
        this.status = status;
        this.error = error;
    }

    @Override
    public ParticipantStatus status() {
        return status;
    }

    @Override
    public String error() {
        return error;
    }
}
