package io.narayana.lra.callbacks.contracts.http;

import io.narayana.lra.api.LRA;
import io.narayana.lra.callbacks.contracts.common.AfterLRACallback;
import io.narayana.lra.callbacks.contracts.common.ParticipantReply;
import java.net.URI;

public class HttpAfterLRACallback {
    public static final String TYPE = "after";

    public static class Request extends AfterLRACallback.Request {
        public Request() {
        }

        public Request(String participantId, URI lraId, URI parentId, String recoveryUrl,
                String compensatorData, LRA.TransactionStatus endStatus) {
            super(participantId, lraId, parentId, recoveryUrl, compensatorData, endStatus);
        }
    }

    public static class Reply extends AfterLRACallback.Reply {
        public Reply() {
        }

        public static Reply from(ParticipantReply reply) {
            Reply converted = new Reply();
            converted.error = reply.error;
            return converted;
        }
    }
}
