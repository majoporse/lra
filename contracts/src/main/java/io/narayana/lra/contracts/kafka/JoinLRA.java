package io.narayana.lra.contracts.kafka;

public class JoinLRA {
    public static class Request extends LRAKafkaRequest {
        public String lraId;
        public Long timeLimit;
        public String compensateLink;
        public String completeLink;
        public String forgetLink;
        public String leaveLink;
        public String afterLink;
        public String statusLink;
        public String compensatorData;

        public Request() {
        }

        public Request(String correlationId, String replyTopic, String lraId, Long timeLimit,
                String compensateLink, String completeLink, String forgetLink,
                String leaveLink, String afterLink, String statusLink,
                String compensatorData) {
            super(correlationId, replyTopic);
            this.lraId = lraId;
            this.timeLimit = timeLimit;
            this.compensateLink = compensateLink;
            this.completeLink = completeLink;
            this.forgetLink = forgetLink;
            this.leaveLink = leaveLink;
            this.afterLink = afterLink;
            this.statusLink = statusLink;
            this.compensatorData = compensatorData;
        }
    }

    public static class Reply extends LRAKafkaReply {
        public String recoveryUrl;
        public String previousCompensatorData;

        public Reply() {
        }

        public Reply(String correlationId, String recoveryUrl, String previousCompensatorData, String error) {
            super(correlationId, error);
            this.recoveryUrl = recoveryUrl;
            this.previousCompensatorData = previousCompensatorData;
        }
    }
}
