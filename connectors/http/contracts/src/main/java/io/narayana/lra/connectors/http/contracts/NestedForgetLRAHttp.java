package io.narayana.lra.connectors.http.contracts;

import io.narayana.lra.connectors.common.contracts.NestedForgetLRA;
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
