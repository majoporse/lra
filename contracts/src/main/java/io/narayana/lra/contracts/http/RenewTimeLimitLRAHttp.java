package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.RenewTimeLimitLRA;
import java.net.URI;

public class RenewTimeLimitLRAHttp {
    public static class Request extends RenewTimeLimitLRA.Request {
        public Request() {
        }

        public Request(URI lraId, long timeLimit) {
            super(lraId, timeLimit);
        }
    }

    public static class Reply extends RenewTimeLimitLRA.Reply {
        public Reply() {
        }

        public Reply(String lraId) {
            super(lraId);
        }
    }
}
