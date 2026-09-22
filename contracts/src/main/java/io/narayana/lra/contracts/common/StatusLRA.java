package io.narayana.lra.contracts.common;

import java.util.UUID;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class StatusLRA {
    public static class Request {
        public UUID lraId;

        public Request() {
        }

        public Request(UUID lraId) {
            this.lraId = lraId;
        }

        public Request(String correlationId, String replyTopic, UUID lraId) {
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
