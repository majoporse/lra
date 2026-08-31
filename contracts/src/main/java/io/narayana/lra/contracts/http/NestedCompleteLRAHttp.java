package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.CloseLRA;
import io.narayana.lra.contracts.common.NestedCompleteLRA;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

import java.net.URI;

public class NestedCompleteLRAHttp {
    public static class Request extends NestedCompleteLRA.Request {
        public Request() {
        }

        public Request(URI nestedLRAId) {
            super(nestedLRAId);
        }
    }

    public static class Reply extends NestedCompleteLRA.Reply {

        public Reply() {
            super();
        }
    }
}
