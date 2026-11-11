package io.narayana.lra.connectors.http.contracts;

import io.narayana.lra.connectors.common.contracts.StatusLRA;
import java.util.UUID;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class StatusLRAHttp {
    public static class Request extends StatusLRA.Request {
        public Request() {
        }

        public Request(UUID lraId) {
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
