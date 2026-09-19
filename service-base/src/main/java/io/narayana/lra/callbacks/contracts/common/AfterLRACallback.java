package io.narayana.lra.callbacks.contracts.common;

import io.narayana.lra.callbacks.CallbackResult;
import java.util.UUID;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

/**
 * Common contract for a participant {@code @AfterLRA} notification.
 */
public class AfterLRACallback {
    public static class Request extends ParticipantRequest {
        public LRAStatus endStatus;

        public Request() {
        }

        public Request(UUID lraId, UUID parentId, String recoveryUrl, String compensatorData,
                String participantId, LRAStatus endStatus) {
            super(lraId, parentId, recoveryUrl, compensatorData, participantId);
            this.endStatus = endStatus;
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
