package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.CloseLRA;

public class CloseLRAHttp {
    public static class Request extends CloseLRA.Request {
        public Request() {
        }

        public Request(String lraId, String compensator, String userData) {
            super(lraId, compensator, userData);
        }
    }

    public static class Reply extends CloseLRA.Reply {
        public Reply() {
        }

        public Reply(String status) {
            super(status, null);
        }
    }
}
