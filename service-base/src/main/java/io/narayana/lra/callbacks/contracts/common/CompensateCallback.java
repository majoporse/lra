package io.narayana.lra.callbacks.contracts.common;

import io.narayana.lra.api.LRA;
import java.net.URI;

public class CompensateCallback {
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

        public Reply(LRA.ParticipantOutcome status) {
            super(status);
        }

        public Reply(LRA.ParticipantOutcome status, String error) {
            super(status, error);
        }
    }
}
