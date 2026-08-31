package io.narayana.lra.contracts.common;

import java.net.URI;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

public class NestedCompleteLRA {
    public static class Request {
        public URI nestedLraId;

        public Request() {
        }

        public Request(URI nestedLraId) {
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
