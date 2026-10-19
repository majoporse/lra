package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.NestedStatusLRA;
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
