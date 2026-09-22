package io.narayana.lra.contracts.common;

import java.util.UUID;

public class NestedForgetLRA {
    public static class Request {
        public UUID nestedLraId;

        public Request() {
        }

        public Request(UUID nestedLraId) {
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
