package io.narayana.lra.contracts.common;

import java.util.UUID;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

public class NestedCompensateLRA {
    public static class Request {
        public UUID nestedLraId;

        public Request() {
        }

        public Request(UUID nestedLraId) {
            this.nestedLraId = nestedLraId;
        }
    }

    public static class Reply {
        public ParticipantStatus status;

        public Reply() {
        }

        public Reply(ParticipantStatus status) {
            this.status = status;
        }
    }
}
