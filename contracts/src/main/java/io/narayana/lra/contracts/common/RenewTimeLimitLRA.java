package io.narayana.lra.contracts.common;

import java.net.URI;

public class RenewTimeLimitLRA {
    public static class Request {
        public URI lraId;
        public long timeLimit;

        public Request() {
        }

        public Request(URI lraId, long timeLimit) {
            this.lraId = lraId;
            this.timeLimit = timeLimit;
        }
    }

    public static class Reply {
        public String lraId;

        public Reply() {
        }

        public Reply(String lraId) {
            this.lraId = lraId;
        }
    }
}
