package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.CloseLRA;
import io.narayana.lra.contracts.common.NestedCompensateLRA;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

import java.net.URI;

public class NestedCompensateLRAHttp {
    public static class Request extends NestedCompensateLRA.Request {
        public Request() {
        }

        public Request(URI nestedLRAId) {
            super(nestedLRAId);
        }
    }

    public static class Reply extends NestedCompensateLRA.Reply {

        public Reply() {
            super();
        }
    }
}
