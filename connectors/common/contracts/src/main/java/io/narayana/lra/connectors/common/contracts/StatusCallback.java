package io.narayana.lra.connectors.common.contracts;

import java.net.URI;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

public class StatusCallback {
    public static class Request extends ParticipantRequest {
        public Request() {
        }

        public Request(URI lraId, URI parentId, String recoveryUrl, String compensatorData) {
            super(lraId, parentId, recoveryUrl, compensatorData);
        }

        public Request(String participantId, URI lraId, URI parentId, String recoveryUrl,
                String compensatorData) {
            super(participantId, lraId, parentId, recoveryUrl, compensatorData);
        }
    }

    public static class Reply extends ParticipantReply {
        public Reply() {
        }

        public Reply(ParticipantStatus status) {
            super(status);
        }

        public Reply(ParticipantStatus status, String error) {
            super(status, error);
        }
    }
}
