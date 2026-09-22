package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.CloseLRA;
import java.util.UUID;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class CloseLRAHttp {
    public static class Request extends CloseLRA.Request {
        public Request() {
        }

        public Request(UUID lraId, String participantId, String userData) {
            super(lraId, participantId, userData);
        }
    }

    public static class Reply extends CloseLRA.Reply {
        public Reply() {
        }

        public Reply(LRAStatus status) {
            super(status, null);
        }
    }
}
