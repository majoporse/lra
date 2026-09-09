package io.narayana.lra.contracts.common;

import java.net.URI;

public class LeaveLRA {
    public static class Request {
        public URI lraId;
        public String participantId;

        public Request() {
        }

        public Request(URI lraId, String participantId) {
            this.lraId = lraId;
            this.participantId = participantId;
        }
    }

    public static class Reply {

        public String status;

        public Reply() {
        }

        public Reply(String status) {
            this.status = status;
        }
    }
}
