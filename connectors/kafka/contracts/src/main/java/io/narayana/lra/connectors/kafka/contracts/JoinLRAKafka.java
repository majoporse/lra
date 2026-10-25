package io.narayana.lra.connectors.kafka.contracts;

import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.contracts.JoinLRA;
import java.util.UUID;

public class JoinLRAKafka {
    public static final String TYPE = "join";

    public static class Request extends JoinLRA.Request implements KafkaRequest<Reply> {
        public String correlationId = "";
        public String replyTopic = "";

        public Request() {
        }

        public Request(String correlationId, String replyTopic, UUID lraId, Long timeLimit,
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

        @Override
        public String getType() {
            return TYPE;
        }

        @Override
        public Class<Reply> getReplyType() {
            return Reply.class;
        }
    }

    public static class Reply extends JoinLRA.Reply implements KafkaReply {
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
        public String getType() {
            return TYPE + "-reply";
        }

        @Override
        public String getError() {
            return error;
        }
    }
}
