package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.NestedForgetLRA;
import java.net.URI;

public class NestedForgetLRAHttp {
    public static class Request extends NestedForgetLRA.Request {
        public Request() {
        }

        public Request(URI nestedLRAId) {
            super(nestedLRAId);
        }
    }

    public static class Reply extends NestedForgetLRA.Reply {
        public Reply() {
            super();
        }

        public Reply(String status) {
            super(status);
        }
    }
}
