package io.narayana.lra.contracts.common;

import java.net.URI;

public class CancelLRA {
    public static class Request {
        public URI lraId;
        public String compensator;
        public String userData;

        public Request() {
        }

        public Request(URI lraId, String compensator, String userData) {
            this.lraId = lraId;
            this.compensator = compensator;
            this.userData = userData;
        }
    }

    public static class Reply {
        public String status;

        public Reply() {
        }

        public Reply(String status, String error) {
            this.status = status;
        }
    }
}
