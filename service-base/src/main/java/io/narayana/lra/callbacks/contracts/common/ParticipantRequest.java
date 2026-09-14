package io.narayana.lra.callbacks.contracts.common;

import java.net.URI;

/**
 * Transport-agnostic base for the context carried by a participant
 * notification: the LRA, its parent, the recovery URL and any participant data.
 */
public class ParticipantRequest {
    public URI lraId;
    public URI parentId;
    public String recoveryUrl;
    public String compensatorData;

    public ParticipantRequest() {
    }

    protected ParticipantRequest(URI lraId, URI parentId, String recoveryUrl, String compensatorData) {
        this.lraId = lraId;
        this.parentId = parentId;
        this.recoveryUrl = recoveryUrl;
        this.compensatorData = compensatorData;
    }
}
