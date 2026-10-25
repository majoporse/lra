package io.narayana.lra.connectors.common.contracts;

import io.narayana.lra.api.LRA;
import java.net.URI;

public class AfterLRACallback {
    public static class Request extends ParticipantRequest {
        public LRA.TransactionStatus endStatus;

        public Request() {
        }

        public Request(URI lraId, URI parentId, String recoveryUrl, String compensatorData,
                LRA.TransactionStatus endStatus) {
            super(lraId, parentId, recoveryUrl, compensatorData);
            this.endStatus = endStatus;
        }

        public Request(String participantId, URI lraId, URI parentId, String recoveryUrl,
                String compensatorData, LRA.TransactionStatus endStatus) {
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
