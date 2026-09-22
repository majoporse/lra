package io.narayana.lra.contracts.common;

import java.util.UUID;

public class LeaveLRA {
    public static class Request {
        public UUID lraId;
        public String participantId;

        public Request() {
        }

        public Request(UUID lraId, String participantId) {
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
