package io.narayana.lra.connectors.http.contracts;

import io.narayana.lra.connectors.common.contracts.NestedCompleteLRA;
import java.util.UUID;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

public class NestedCompleteLRAHttp {
    public static class Request extends NestedCompleteLRA.Request {
        public Request() {
        }

        public Request(UUID nestedLRAId) {
            super(nestedLRAId);
        }
    }

    public static class Reply extends NestedCompleteLRA.Reply {
        public Reply() {
            super();
        }

        public Reply(ParticipantStatus status) {
            super(status);
        }
    }
}
