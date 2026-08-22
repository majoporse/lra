package io.narayana.lra.contracts.common;

public class StartLRA {
    public static class Request {
        public String clientId;
        public Long timeout;
        public String parentLRA;

        public Request() {
        }

        public Request(String clientId, Long timeout, String parentLRA) {
            this.clientId = clientId;
            this.timeout = timeout;
            this.parentLRA = parentLRA;
        }

        public Request(String correlationId, String replyTopic, String clientId, Long timeout, String parentLRA) {
            this(clientId, timeout, parentLRA);
        }
    }

    public static class Reply {
        public String lraId;

        public Reply() {
        }

        public Reply(String lraId, String error) {
            this.lraId = lraId;
        }

        public Reply(String correlationId, String lraId, String error) {
            this(lraId, error);
        }
    }
}
