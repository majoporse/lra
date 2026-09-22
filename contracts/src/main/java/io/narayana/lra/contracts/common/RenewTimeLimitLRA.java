package io.narayana.lra.contracts.common;

import java.util.UUID;

public class RenewTimeLimitLRA {
    public static class Request {
        public UUID lraId;
        public long timeLimit;

        public Request() {
        }

        public Request(UUID lraId, long timeLimit) {
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
