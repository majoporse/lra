package io.narayana.lra.callbacks.contracts.kafka;

import io.narayana.lra.callbacks.contracts.common.Forget;
import java.net.URI;

/**
 * Kafka transport for the participant {@code @Forget} notification.
 */
public class ForgetKafka {
    public static final String TYPE = "forget";

    public static class Request extends Forget.Request {
        public String correlationId = "";
        public String replyTopic = "";

        public Request() {
        }

        public Request(String correlationId, String replyTopic, URI lraId, URI parentId,
                String recoveryUrl, String compensatorData) {
            super(lraId, parentId, recoveryUrl, compensatorData);
            this.correlationId = correlationId;
            this.replyTopic = replyTopic;
        }

        public String getCorrelationId() {
            return correlationId;
        }

        public String getReplyTopic() {
            return replyTopic;
        }
    }

    public static class Reply extends Forget.Reply {
        public String correlationId;
        public String error;

        public Reply() {
        }

        public Reply(String correlationId, String error) {
            super();
            this.correlationId = correlationId;
            this.error = error;
        }

        public String getCorrelationId() {
            return correlationId;
        }

        public String getError() {
            return error;
        }
    }
}
