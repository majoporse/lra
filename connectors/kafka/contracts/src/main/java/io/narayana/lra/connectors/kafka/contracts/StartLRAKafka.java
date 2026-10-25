package io.narayana.lra.connectors.kafka.contracts;

import io.narayana.lra.connectors.common.contracts.StartLRA;
import java.util.UUID;

public class StartLRAKafka {
    public static final String TYPE = "start";

    public static class Request extends StartLRA.Request implements KafkaRequest<Reply> {
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

        @Override
        public String getType() {
            return TYPE;
        }

        @Override
        public Class<Reply> getReplyType() {
            return Reply.class;
        }
    }

    public static class Reply extends StartLRA.Reply implements KafkaReply {
        public String correlationId;
        public String error;

        public Reply() {
        }

        public Reply(String correlationId, UUID lraId, String error) {
            super(lraId, null, error);
            this.correlationId = correlationId;
            this.error = error;
        }

        @Override
        public String getCorrelationId() {
            return correlationId;
        }

        @Override
        public String getType() {
            return TYPE + "-reply";
        }

        @Override
        public String getError() {
            return error;
        }
    }
}
