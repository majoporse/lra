package io.narayana.lra.contracts.common;

public class LeaveLRA {
    public static class Request {
        public String lraId;
        public String body;

        public Request() {
        }

        public Request(String lraId, String body) {
            this.lraId = lraId;
            this.body = body;
        }

        public Request(String correlationId, String replyTopic, String lraId, String body) {
            this(lraId, body);
        }
    }

    public static class Reply {

        public Reply() {
        }

        public Reply(String error) {
        }

        public Reply(String correlationId, String error) {
            this(error);
        }
    }
}
