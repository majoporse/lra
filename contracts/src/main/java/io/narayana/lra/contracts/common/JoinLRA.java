package io.narayana.lra.contracts.common;

public class JoinLRA {
    public static class Request {
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

        public Request(String lraId, Long timeLimit,
                String compensateLink, String completeLink, String forgetLink,
                String leaveLink, String afterLink, String statusLink,
                String compensatorData) {
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

        public Request(String correlationId, String replyTopic, String lraId, Long timeLimit,
                String compensateLink, String completeLink, String forgetLink,
                String leaveLink, String afterLink, String statusLink,
                String compensatorData) {
            this(lraId, timeLimit, compensateLink, completeLink, forgetLink,
                    leaveLink, afterLink, statusLink, compensatorData);
        }
    }

    public static class Reply {
        public String recoveryUrl;
        public String previousCompensatorData;

        public Reply() {
        }

        public Reply(String recoveryUrl, String previousCompensatorData, String error) {
            this.recoveryUrl = recoveryUrl;
            this.previousCompensatorData = previousCompensatorData;
        }

        public Reply(String correlationId, String recoveryUrl, String previousCompensatorData, String error) {
            this(recoveryUrl, previousCompensatorData, error);
        }
    }
}
