package io.narayana.lra.contracts.common;

public class StatusLRA {
    public static class Request {
        public String lraId;

        public Request() {
        }

        public Request(String lraId) {
            this.lraId = lraId;
        }

        public Request(String correlationId, String replyTopic, String lraId) {
            this(lraId);
        }
    }

    public static class Reply {
        public String status;

        public Reply() {
        }

        public Reply(String status, String error) {
            this.status = status;
        }

        public Reply(String correlationId, String status, String error) {
            this(status, error);
        }
    }
}
