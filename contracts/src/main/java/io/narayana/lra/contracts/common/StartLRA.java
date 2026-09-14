package io.narayana.lra.contracts.common;

import java.net.URI;
import java.util.UUID;

public class StartLRA {
    public static class Request {
        public String clientId;
        public Long timeout;
        public UUID parentLRA;

        public Request() {
        }

        public Request(String clientId, Long timeout, UUID parentLRA) {
            this.clientId = clientId;
            this.timeout = timeout;
            this.parentLRA = parentLRA;
        }
    }

    public static class Reply {
        public URI lraId;
        public UUID parent;
        public String error;

        public Reply() {
        }

        public Reply(URI lraId, UUID parent, String error) {
            this.lraId = lraId;

            this.parent = parent;
            this.error = error;
        }
    }
}
