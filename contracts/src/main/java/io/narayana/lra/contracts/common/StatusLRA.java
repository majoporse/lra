package io.narayana.lra.contracts.common;

import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class StatusLRA {
    public static class Request {
        public String lraId;

        public Request() {
        }

        public Request(String lraId) {
            this.lraId = lraId;
        }

        public Request(String correlationId, String replyTopic, String lraId) {
            this(lraId);
        }
    }

    public static class Reply {
        public LRAStatus status;

        public Reply() {
        }

        public Reply(LRAStatus status, String error) {
            this.status = status;
        }
    }
}
