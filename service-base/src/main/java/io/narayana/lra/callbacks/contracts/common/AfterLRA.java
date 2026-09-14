package io.narayana.lra.callbacks.contracts.common;

import java.net.URI;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

/**
 * Common contract for a participant {@code @AfterLRA} notification.
 */
public class AfterLRA {
    public static class Request extends ParticipantRequest {
        public LRAStatus endStatus;

        public Request() {
        }

        public Request(URI lraId, URI parentId, String recoveryUrl, String compensatorData, LRAStatus endStatus) {
            super(lraId, parentId, recoveryUrl, compensatorData);
            this.endStatus = endStatus;
        }
    }

    public static class Reply extends ParticipantReply {
        public Reply() {
            super(null);
        }
    }
}
