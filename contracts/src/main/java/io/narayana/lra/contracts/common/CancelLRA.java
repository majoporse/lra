package io.narayana.lra.contracts.common;

public class CancelLRA {
    public static class Request {
        public String lraId;
        public String compensator;
        public String userData;

        public Request() {
        }

        public Request(String lraId, String compensator, String userData) {
            this.lraId = lraId;
            this.compensator = compensator;
            this.userData = userData;
        }

        public Request(String correlationId, String replyTopic, String lraId, String compensator, String userData) {
            this(lraId, compensator, userData);
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
