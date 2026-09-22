package io.narayana.lra.callbacks;

public class CallbackContext {
    private final String lraId;
    private final String parentId;
    private final String recoveryId;
    private final String compensatorData;
    private final String payload;

    public CallbackContext(String lraId, String parentId, String recoveryId, String compensatorData) {
        this(lraId, parentId, recoveryId, compensatorData, null);
    }

    public CallbackContext(String lraId, String parentId, String recoveryId, String compensatorData, String payload) {
        this.lraId = lraId;
        this.parentId = parentId;
        this.recoveryId = recoveryId;
        this.compensatorData = compensatorData;
        this.payload = payload;
    }

    public String getLraId() {
        return lraId;
    }

    public String getParentId() {
        return parentId;
    }

    public String getRecoveryId() {
        return recoveryId;
    }

    public String getCompensatorData() {
        return compensatorData;
    }

    public String getPayload() {
        return payload;
    }
}
