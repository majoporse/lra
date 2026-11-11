package io.narayana.lra.connectors.http.contracts;

import io.narayana.lra.connectors.common.contracts.CompleteCallback;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import java.net.URI;

public class HttpCompleteCallback {
    public static final String TYPE = "complete";

    public static class Request extends CompleteCallback.Request {
        public Request() {
        }

        public Request(String participantId, URI lraId, URI parentId, String recoveryUrl,
                String compensatorData) {
            super(participantId, lraId, parentId, recoveryUrl, compensatorData);
        }
    }

    public static class Reply extends CompleteCallback.Reply {
        public Reply() {
        }

        public static Reply from(ParticipantReply reply) {
            Reply converted = new Reply();
            converted.status = reply.status;
            converted.error = reply.error;
            return converted;
        }
    }
}
