package io.narayana.lra.contracts.kafka;

import io.narayana.lra.contracts.common.LeaveLRA;
import java.util.UUID;

public class LeaveLRAKafka {
    public static final String TYPE = "leave";

    public static class Request extends LeaveLRA.Request implements LRAKafkaRequest {
        public String correlationId = "";
        public String replyTopic = "";

        public Request() {
        }

        public Request(String correlationId, String replyTopic, UUID lraId, String body) {
            super(lraId, body);
            this.correlationId = correlationId;
            this.replyTopic = replyTopic;
        }

        @Override
        public String getCorrelationId() {
            return correlationId;
        }

        @Override
        public String getReplyTopic() {
            return replyTopic;
        }
    }

    public static class Reply extends LeaveLRA.Reply implements LRAKafkaReply {
        public String correlationId;
        public String error;

        public Reply() {
        }

        public Reply(String correlationId, String error) {
            super(error);
            this.correlationId = correlationId;
            this.error = error;
        }

        @Override
        public String getCorrelationId() {
            return correlationId;
        }

        @Override
        public String getError() {
            return error;
        }
    }
}
