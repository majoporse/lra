package io.narayana.lra.connectors.kafka.contracts;

import io.narayana.lra.api.LRA;
import io.narayana.lra.connectors.common.contracts.AfterLRACallback;
import java.net.URI;

/**
 * Kafka transport for the participant {@code @AfterLRA} notification.
 */
public class AfterLRAKafkaCallback {
    public static final String TYPE = "afterLRA";

    public static class Request extends AfterLRACallback.Request implements KafkaRequest<Reply> {
        public String correlationId = "";
        public String replyTopic = "";

        public Request() {
        }

        public Request(String correlationId, String replyTopic, URI lraId, URI parentId,
                String recoveryUrl, String compensatorData, LRA.TransactionStatus endStatus) {
            this(correlationId, replyTopic, null, lraId, parentId, recoveryUrl, compensatorData, endStatus);
        }

        public Request(String correlationId, String replyTopic, String participantId, URI lraId, URI parentId,
                String recoveryUrl, String compensatorData, LRA.TransactionStatus endStatus) {
            super(participantId, lraId, parentId, recoveryUrl, compensatorData, endStatus);
            this.correlationId = correlationId;
            this.replyTopic = replyTopic;
        }

        public String getCorrelationId() {
            return correlationId;
        }

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

    public static class Reply extends AfterLRACallback.Reply implements KafkaCallbackReply {
        public String correlationId;

        public Reply() {
        }

        public Reply(String correlationId, String error) {
            super(error);
            this.correlationId = correlationId;
        }

        public String getCorrelationId() {
            return correlationId;
        }

        @Override
        public String getType() {
            return TYPE + "-reply";
        }

        @Override
        public LRA.ParticipantOutcome getStatus() {
            return status;
        }

        public String getError() {
            return error;
        }
    }
}
