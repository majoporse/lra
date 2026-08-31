package io.narayana.lra.contracts.common;

import java.net.URI;

public class NestedCompensateLRA {
    public static class Request {
        public URI nestedLraId;

        public Request() {
        }

        public Request(URI nestedLraId) {
            this.nestedLraId = nestedLraId;
        }
    }

    public static class Reply {
        public String status;

        public Reply() {
        }

        public Reply(String status) {
            this.status = status;
        }
    }
}
