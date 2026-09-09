package io.narayana.lra.contracts.kafka;

import java.net.URI;
import java.util.ArrayList;

public class StartLRAKafka {
    public static class Request extends io.narayana.lra.contracts.common.StartLRA.Request implements LRAKafkaRequest {
        public String correlationId = "";
        public String replyTopic = "";

        public Request() {
        }

        public Request(String correlationId, String replyTopic, String clientId, Long timeout, URI parentLRA) {
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

    public static class Reply extends io.narayana.lra.contracts.common.StartLRA.Reply implements LRAKafkaReply {
        public String correlationId;
        public String error;

        public Reply() {
        }

        public Reply(String correlationId, URI lraId, String error) {
            super(lraId, new ArrayList<>(), error);
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
