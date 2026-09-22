package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.NestedCompensateLRA;
import java.util.UUID;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

public class NestedCompensateLRAHttp {
    public static class Request extends NestedCompensateLRA.Request {
        public Request() {
        }

        public Request(UUID nestedLRAId) {
            super(nestedLRAId);
        }
    }

    public static class Reply extends NestedCompensateLRA.Reply {

        public Reply(ParticipantStatus status) {
            super(status);
        }

        public Reply() {
            super();
        }
    }
}
