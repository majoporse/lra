package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.NestedCompleteLRA;
import java.net.URI;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

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

        public Reply(ParticipantStatus status) {
            super(status);
        }
    }
}
