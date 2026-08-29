package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.CancelLRA;

public class CancelLRAHttp {
    public static class Request extends CancelLRA.Request {
        public Request() {
        }

        public Request(String lraId, String compensator, String userData) {
            super(lraId, compensator, userData);
        }
    }

    public static class Reply extends CancelLRA.Reply {
        public Reply() {
        }

        public Reply(String status) {
            super(status, null);
        }
    }
}
