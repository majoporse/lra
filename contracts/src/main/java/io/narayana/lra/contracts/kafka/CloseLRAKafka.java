package io.narayana.lra.contracts.kafka;

import java.net.URI;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class CloseLRAKafka {
    public static class Request extends io.narayana.lra.contracts.common.CloseLRA.Request implements LRAKafkaRequest {
        public String correlationId = "";
        public String replyTopic = "";

        public Request() {
        }

        public Request(String correlationId, String replyTopic, URI lraId, String compensator, String userData) {
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
    }

    public static class Reply extends io.narayana.lra.contracts.common.CloseLRA.Reply implements LRAKafkaReply {
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
        public String getError() {
            return error;
        }
    }
}
