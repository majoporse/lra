package io.narayana.lra.contracts.kafka;

import io.narayana.lra.contracts.common.StartLRA;
import java.net.URI;
import java.util.UUID;

public class StartLRAKafka {
    public static class Request extends StartLRA.Request implements LRAKafkaRequest {
        public String correlationId = "";
        public String replyTopic = "";

        public Request() {
        }

        public Request(String correlationId, String replyTopic, String clientId, Long timeout, UUID parentLRA) {
            super(clientId, timeout, parentLRA);
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

    public static class Reply extends StartLRA.Reply implements LRAKafkaReply {
        public String correlationId;
        public String error;

        public Reply() {
        }

        public Reply(String correlationId, URI lraId, String error) {
            super(lraId, null, error);
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
