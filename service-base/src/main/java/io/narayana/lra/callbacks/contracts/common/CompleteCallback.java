package io.narayana.lra.callbacks.contracts.common;

import java.net.URI;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

/**
 * Common contract for a participant {@code @Complete} notification.
 */
public class CompleteCallback {
    public static class Request extends ParticipantRequest {
        public Request() {
        }

        public Request(URI lraId, URI parentId, String recoveryUrl, String compensatorData) {
            super(lraId, parentId, recoveryUrl, compensatorData);
        }
    }

    public static class Reply extends ParticipantReply {
        public Reply() {
        }

        public Reply(ParticipantStatus status) {
            super(status);
        }
    }
}
