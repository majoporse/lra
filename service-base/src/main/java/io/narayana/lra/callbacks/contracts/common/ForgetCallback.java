package io.narayana.lra.callbacks.contracts.common;

import java.net.URI;

/**
 * Common contract for a participant {@code @Forget} notification.
 */
public class ForgetCallback {
    public static class Request extends ParticipantRequest {
        public Request() {
        }

        public Request(URI lraId, URI parentId, String recoveryUrl, String compensatorData) {
            super(lraId, parentId, recoveryUrl, compensatorData);
        }
    }

    public static class Reply extends ParticipantReply {
        public Reply() {
            super(null);
        }
    }
}
