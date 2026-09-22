package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.LeaveLRA;
import java.util.UUID;

public class LeaveLRAHttp {
    public static class Request extends LeaveLRA.Request {
        public Request() {
        }

        public Request(UUID lraId, String participantId) {
            super(lraId, participantId);
        }
    }

    public static class Reply extends LeaveLRA.Reply {
        public Reply() {

        }

        public Reply(String status) {
            super(status);
        }
    }
}
