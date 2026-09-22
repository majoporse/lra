package io.narayana.lra.contracts.common;

import java.util.UUID;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class CloseLRA {
    public static class Request {
        public UUID lraId;
        public String participantId;
        public String userData;

        public Request() {
        }

        public Request(UUID lraId, String participantId, String userData) {
            this.lraId = lraId;
            this.participantId = participantId;
            this.userData = userData;
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
