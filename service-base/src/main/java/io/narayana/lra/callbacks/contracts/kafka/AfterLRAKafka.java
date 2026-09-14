package io.narayana.lra.callbacks.contracts.kafka;

import io.narayana.lra.callbacks.contracts.common.AfterLRA;
import java.net.URI;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

/**
 * Kafka transport for the participant {@code @AfterLRA} notification.
 */
public class AfterLRAKafka {
    public static final String TYPE = "afterLRA";

    public static class Request extends AfterLRA.Request {
        public String correlationId = "";
        public String replyTopic = "";

        public Request() {
        }

        public Request(String correlationId, String replyTopic, URI lraId, URI parentId,
                String recoveryUrl, String compensatorData, LRAStatus endStatus) {
            super(lraId, parentId, recoveryUrl, compensatorData, endStatus);
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

    public static class Reply extends AfterLRA.Reply {
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
