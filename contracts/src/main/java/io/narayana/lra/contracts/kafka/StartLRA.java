package io.narayana.lra.contracts.kafka;

public class StartLRA {
    public static class Request extends LRAKafkaRequest {
        public String clientId;
        public Long timeout;
        public String parentLRA;

        public Request() {
        }

        public Request(String correlationId, String replyTopic, String clientId, Long timeout, String parentLRA) {
            super(correlationId, replyTopic);
            this.clientId = clientId;
            this.timeout = timeout;
            this.parentLRA = parentLRA;
        }
    }

    public static class Reply extends LRAKafkaReply {
        public String lraId;

        public Reply() {
        }

        public Reply(String correlationId, String lraId, String error) {
            super(correlationId, error);
            this.lraId = lraId;
        }
    }
}
