package io.narayana.lra.callbacks.contracts.common;

import java.util.UUID;

/**
 * Transport-agnostic base for the context carried by a participant
 * notification: the LRA, its parent, the recovery URL and any participant data.
 */
public class ParticipantRequest {
    public UUID lraId;
    public UUID parentId;
    public String recoveryUrl;
    public String compensatorData;
    public String participantId;

    public ParticipantRequest() {
    }

    protected ParticipantRequest(UUID lraId, UUID parentId, String recoveryUrl, String compensatorData,
            String participantId) {
        this.lraId = lraId;
        this.parentId = parentId;
        this.recoveryUrl = recoveryUrl;
        this.compensatorData = compensatorData;
        this.participantId = participantId;
    }
}
