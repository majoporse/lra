package io.narayana.lra.connectors.common.contracts;

import java.net.URI;

public class ParticipantRequest {
    public String participantId;
    public URI lraId;
    public URI parentId;
    public String recoveryUrl;
    public String compensatorData;

    public ParticipantRequest() {
    }

    protected ParticipantRequest(URI lraId, URI parentId, String recoveryUrl, String compensatorData) {
        this(null, lraId, parentId, recoveryUrl, compensatorData);
    }

    protected ParticipantRequest(String participantId, URI lraId, URI parentId, String recoveryUrl,
            String compensatorData) {
        this.participantId = participantId;
        this.lraId = lraId;
        this.parentId = parentId;
        this.recoveryUrl = recoveryUrl;
        this.compensatorData = compensatorData;
    }
}
