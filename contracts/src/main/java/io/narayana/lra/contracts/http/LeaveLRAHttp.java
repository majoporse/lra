package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.LeaveLRA;
import java.net.URI;

public class LeaveLRAHttp {
    public static class Request extends LeaveLRA.Request {
        public Request() {
        }

        public Request(URI lraId, String participantId) {
            super(lraId, participantId);
        }
    }

    public static class Reply extends LeaveLRA.Reply {
        public Reply() {
            super();
        }
    }
}
