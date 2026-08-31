package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.CloseLRA;
import io.narayana.lra.contracts.common.NestedStatusLRA;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

import java.net.URI;

public class NestedStatusLRAHttp {
    public static class Request extends NestedStatusLRA.Request {
        public Request() {
        }

        public Request(URI nestedLRAId) {
            super(nestedLRAId);
        }
    }

    public static class Reply extends NestedStatusLRA.Reply {
        public Reply() {
        }
    }
}
