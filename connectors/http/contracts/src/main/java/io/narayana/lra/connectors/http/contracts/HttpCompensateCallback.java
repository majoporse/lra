package io.narayana.lra.connectors.http.contracts;

import io.narayana.lra.connectors.common.contracts.CompensateCallback;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import java.net.URI;

public class HttpCompensateCallback {
    public static final String TYPE = "compensate";

    public static class Request extends CompensateCallback.Request {
        public Request() {
        }

        public Request(String participantId, URI lraId, URI parentId, String recoveryUrl,
                String compensatorData) {
            super(participantId, lraId, parentId, recoveryUrl, compensatorData);
        }
    }

    public static class Reply extends CompensateCallback.Reply {
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
