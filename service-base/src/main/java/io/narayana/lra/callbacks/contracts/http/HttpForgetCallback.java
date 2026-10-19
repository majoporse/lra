package io.narayana.lra.callbacks.contracts.http;

import io.narayana.lra.callbacks.contracts.common.ForgetCallback;
import io.narayana.lra.callbacks.contracts.common.ParticipantReply;
import java.net.URI;

public class HttpForgetCallback {
    public static final String TYPE = "forget";

    public static class Request extends ForgetCallback.Request {
        public Request() {
        }

        public Request(String participantId, URI lraId, URI parentId, String recoveryUrl,
                String compensatorData) {
            super(participantId, lraId, parentId, recoveryUrl, compensatorData);
        }
    }

    public static class Reply extends ForgetCallback.Reply {
        public Reply() {
        }

        public static Reply from(ParticipantReply reply) {
            Reply converted = new Reply();
            converted.error = reply.error;
            return converted;
        }
    }
}
