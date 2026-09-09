package io.narayana.lra.contracts.kafka;

import io.narayana.lra.callbacks.ParticipantCallbacks;
import java.net.URI;

public class JoinLRAKafka {
    public static class Request extends io.narayana.lra.contracts.common.JoinLRA.Request implements LRAKafkaRequest {
        public String correlationId = "";
        public String replyTopic = "";

        public Request() {
        }

        public Request(String correlationId, String replyTopic, URI lraId, Long timeLimit,
                ParticipantCallbacks callbacks,
                String userData, String partId) {
            super(lraId, timeLimit, callbacks, userData, partId);
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

    public static class Reply extends io.narayana.lra.contracts.common.JoinLRA.Reply implements LRAKafkaReply {
        public String correlationId;
        public String error;

        public Reply() {
        }

        public Reply(String correlationId, String recoveryUrl, String previousCompensatorData, String error) {
            super(recoveryUrl, previousCompensatorData, error);
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
