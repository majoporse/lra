package io.narayana.lra.connectors.http.contracts;

import io.narayana.lra.connectors.common.contracts.NestedStatusLRA;
import java.util.UUID;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;

public class NestedStatusLRAHttp {
    public static class Request extends NestedStatusLRA.Request {
        public Request() {
        }

        public Request(UUID nestedLRAId) {
            super(nestedLRAId);
        }
    }

    public static class Reply extends NestedStatusLRA.Reply {
        public Reply(ParticipantStatus status) {
            super(status);
        }

        public Reply() {
        }
    }
}
