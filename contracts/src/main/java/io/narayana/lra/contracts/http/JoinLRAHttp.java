package io.narayana.lra.contracts.http;

import io.narayana.lra.callbacks.ParticipantCallbacks;
import io.narayana.lra.contracts.common.JoinLRA;
import java.net.URI;

public class JoinLRAHttp {
    public static class Request extends JoinLRA.Request {

        public Request() {
        }

        public Request(
                URI lraId,
                Long timeLimit,
                ParticipantCallbacks participantCallbacks,
                String userData,
                String partId) {
            super(lraId, timeLimit,
                    participantCallbacks,
                    userData,
                    partId);
        }
    }

    public static class Reply extends JoinLRA.Reply {
        public Reply() {
        }

        public Reply(String recoveryUrl, String previousCompensatorData) {
            super(recoveryUrl, previousCompensatorData, null);
        }
    }
}
