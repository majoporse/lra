package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.LeaveLRA;

public class LeaveLRAHttp {
    public static class Request extends LeaveLRA.Request {
        public Request() {
        }

        public Request(String lraId, String body) {
            super(lraId, body);
        }
    }

    public static class Reply extends LeaveLRA.Reply {
        public Reply() {
        }
    }
}
