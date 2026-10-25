package io.narayana.lra.connectors.http.contracts;

import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.StatusCallback;
import java.net.URI;

public class HttpStatusCallback {
    public static final String TYPE = "status";

    public static class Request extends StatusCallback.Request {
        public Request() {
        }

        public Request(String participantId, URI lraId, URI parentId, String recoveryUrl,
                String compensatorData) {
            super(participantId, lraId, parentId, recoveryUrl, compensatorData);
        }
    }

    public static class Reply extends StatusCallback.Reply {
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
