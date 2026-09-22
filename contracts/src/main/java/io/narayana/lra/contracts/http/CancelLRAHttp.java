package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.CancelLRA;
import java.net.URI;

public class CancelLRAHttp {
    public static class Request extends CancelLRA.Request {
        public Request() {
        }

        public Request(URI lraId, String compensator, String userData) {
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
