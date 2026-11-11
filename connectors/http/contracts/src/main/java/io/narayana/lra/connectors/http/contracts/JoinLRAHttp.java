package io.narayana.lra.connectors.http.contracts;

import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.contracts.JoinLRA;
import java.util.UUID;

public class JoinLRAHttp {
    public static class Request extends JoinLRA.Request {

        public Request() {
        }

        public Request(
                UUID lraId,
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
