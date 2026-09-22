package io.narayana.lra.callbacks.contracts.common;

import io.narayana.lra.callbacks.CallbackResult;
import java.util.UUID;

/**
 * Common contract for a participant {@code @Compensate} notification.
 */
public class CompensateLRACallback {
    public static class Request extends ParticipantRequest {
        public Request() {
        }

        public Request(UUID lraId, UUID parentId, String recoveryUrl, String compensatorData, String participantId) {
            super(lraId, parentId, recoveryUrl, compensatorData, participantId);
        }
    }

    public static class Reply extends ParticipantReply {
        public Reply() {
        }

        public Reply(CallbackResult result) {
            super(result);
        }
    }
}
