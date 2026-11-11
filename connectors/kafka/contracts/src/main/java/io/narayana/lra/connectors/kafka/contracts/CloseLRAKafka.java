package io.narayana.lra.connectors.kafka.contracts;

import io.narayana.lra.connectors.common.contracts.CloseLRA;
import java.util.UUID;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class CloseLRAKafka {
    public static final String TYPE = "close";

    public static class Request extends CloseLRA.Request implements KafkaRequest<Reply> {
        public String correlationId = "";
        public String replyTopic = "";

        public Request() {
        }

        public Request(String correlationId, String replyTopic, UUID lraId, String compensator, String userData) {
            super(lraId, compensator, userData);
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

    public static class Reply extends CloseLRA.Reply implements KafkaReply {
        public String correlationId;
        public String error;

        public Reply() {
        }

        public Reply(String correlationId, LRAStatus status, String error) {
            super(status, error);
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
