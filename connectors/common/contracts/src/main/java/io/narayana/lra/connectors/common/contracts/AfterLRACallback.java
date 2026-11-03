package io.narayana.lra.connectors.common.contracts;

import java.net.URI;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class AfterLRACallback {
    public static class Request extends ParticipantRequest {
        public LRAStatus endStatus;

        public Request() {
        }

        public Request(URI lraId, URI parentId, String recoveryUrl, String compensatorData,
                LRAStatus endStatus) {
            super(lraId, parentId, recoveryUrl, compensatorData);
            this.endStatus = endStatus;
        }

        public Request(String participantId, URI lraId, URI parentId, String recoveryUrl,
                String compensatorData, LRAStatus endStatus) {
            super(participantId, lraId, parentId, recoveryUrl, compensatorData);
            this.endStatus = endStatus;
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
