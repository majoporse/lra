package io.narayana.lra.contracts.kafka;

public class StatusLRA {
    public static class Request extends LRAKafkaRequest {
        public String lraId;

        public Request() {
        }

        public Request(String correlationId, String replyTopic, String lraId) {
            super(correlationId, replyTopic);
            this.lraId = lraId;
        }
    }

    public static class Reply extends LRAKafkaReply {
        public String status;

        public Reply() {
        }

        public Reply(String correlationId, String status, String error) {
            super(correlationId, error);
            this.status = status;
        }
    }
}
