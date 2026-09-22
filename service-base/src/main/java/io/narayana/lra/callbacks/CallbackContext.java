package io.narayana.lra.callbacks;

import java.util.UUID;

public class CallbackContext {
    private final UUID lraId;
    private final UUID parentId;
    private final String recoveryId;
    private final String compensatorData;
    private final String participantId;
    private final String payload;

    public CallbackContext(UUID lraId, UUID parentId, String recoveryId, String compensatorData,
            String participantId) {
        this(lraId, parentId, recoveryId, compensatorData, participantId, null);
    }

    public CallbackContext(UUID lraId, UUID parentId, String recoveryId, String compensatorData,
            String participantId, String payload) {
        this.lraId = lraId;
        this.parentId = parentId;
        this.recoveryId = recoveryId;
        this.compensatorData = compensatorData;
        this.participantId = participantId;
        this.payload = payload;
    }

    public UUID getLraId() {
        return lraId;
    }

    public UUID getParentId() {
        return parentId;
    }

    public String getRecoveryId() {
        return recoveryId;
    }

    public String getCompensatorData() {
        return compensatorData;
    }

    public String getParticipantId() {
        return participantId;
    }

    public String getPayload() {
        return payload;
    }
}