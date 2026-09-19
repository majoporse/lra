package io.narayana.lra.contracts.kafka;

import io.narayana.lra.callbacks.contracts.common.StatusLRACallback;
import java.util.UUID;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class StatusLRAKafka {
    public static class Request extends StatusLRACallback.Request implements LRAKafkaRequest {
        public String correlationId = "";
        public String replyTopic = "";

        public Request() {
        }

        public Request(String correlationId, String replyTopic, String clientId, Long timeout, UUID parentLRA) {
            super(clientId, parentLRA);
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

    public static class Reply extends StatusLRACallback.Reply implements LRAKafkaReply {
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
