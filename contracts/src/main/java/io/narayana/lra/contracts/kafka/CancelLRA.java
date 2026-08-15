package io.narayana.lra.contracts.kafka;

public class CancelLRA {
    public static class Request extends LRAKafkaRequest {
        public String lraId;
        public String compensator;
        public String userData;

        public Request() {
        }

        public Request(String correlationId, String replyTopic, String lraId, String compensator, String userData) {
            super(correlationId, replyTopic);
            this.lraId = lraId;
            this.compensator = compensator;
            this.userData = userData;
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
