package io.narayana.lra.contracts.kafka;

public class LeaveLRA {
    public static class Request extends LRAKafkaRequest {
        public String lraId;
        public String body;

        public Request() {
        }

        public Request(String correlationId, String replyTopic, String lraId, String body) {
            super(correlationId, replyTopic);
            this.lraId = lraId;
            this.body = body;
        }
    }

    public static class Reply extends LRAKafkaReply {

        public Reply() {
        }

        public Reply(String correlationId, String error) {
            super(correlationId, error);
        }
    }
}
