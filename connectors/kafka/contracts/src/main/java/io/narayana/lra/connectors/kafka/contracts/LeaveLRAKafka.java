package io.narayana.lra.connectors.kafka.contracts;

import io.narayana.lra.connectors.common.contracts.LeaveLRA;
import java.util.UUID;

public class LeaveLRAKafka {
    public static final String TYPE = "leave";

    public static class Request extends LeaveLRA.Request implements KafkaRequest<Reply> {
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

        @Override
        public String getType() {
            return TYPE;
        }

        @Override
        public Class<Reply> getReplyType() {
            return Reply.class;
        }
    }

    public static class Reply extends LeaveLRA.Reply implements KafkaReply {
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
        public String getType() {
            return TYPE + "-reply";
        }

        @Override
        public String getError() {
            return error;
        }
    }
}
