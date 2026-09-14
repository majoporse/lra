package io.narayana.lra.callbacks.contracts.kafka;

import io.narayana.lra.callbacks.contracts.common.Compensate;
import java.net.URI;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

/**
 * Kafka transport for the participant {@code @Compensate} notification.
 */
public class CompensateKafka {
    public static final String TYPE = "compensate";

    public static class Request extends Compensate.Request {
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

    public static class Reply extends Compensate.Reply {
        public String correlationId;
        public String error;

        public Reply() {
        }

        public Reply(String correlationId, ParticipantStatus status, String error) {
            super(status);
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
