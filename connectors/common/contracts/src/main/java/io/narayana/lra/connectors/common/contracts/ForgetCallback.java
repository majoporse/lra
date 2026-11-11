package io.narayana.lra.connectors.common.contracts;

import java.net.URI;

public class ForgetCallback {
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
            super(null);
        }

        public Reply(String error) {
            super(null, error);
        }
    }
}
