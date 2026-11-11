package io.narayana.lra.connectors.http.contracts;

import io.narayana.lra.connectors.common.contracts.StartLRA;
import java.util.UUID;

public class StartLRAHttp {
    public static class Request extends StartLRA.Request {
        public Request() {
        }

        public Request(String clientId, Long timeout, UUID parentLRA) {
            super(clientId, timeout, parentLRA);
        }
    }

    public static class Reply extends StartLRA.Reply {
        public Reply() {
        }

        public Reply(UUID lraId, UUID parent) {
            super(lraId, parent, null);
        }
    }
}
