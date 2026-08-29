package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.StatusLRA;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class StatusLRAHttp {
    public static class Request extends StatusLRA.Request {
        public Request() {
        }

        public Request(String lraId) {
            super(lraId);
        }
    }

    public static class Reply extends StatusLRA.Reply {
        public Reply() {
        }

        public Reply(LRAStatus status) {
            super(status, null);
        }
    }
}
