package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.NestedForgetLRA;
import java.util.UUID;

public class NestedForgetLRAHttp {
    public static class Request extends NestedForgetLRA.Request {
        public Request() {
        }

        public Request(UUID nestedLRAId) {
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
