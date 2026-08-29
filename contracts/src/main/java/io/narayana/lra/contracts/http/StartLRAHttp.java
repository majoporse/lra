package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.StartLRA;
import java.net.URI;
import java.util.List;

public class StartLRAHttp {
    public static class Request extends StartLRA.Request {
        public Request() {
        }

        public Request(String clientId, Long timeout, URI parentLRA) {
            super(clientId, timeout, parentLRA);
        }
    }

    public static class Reply extends StartLRA.Reply {
        public Reply() {
        }

        public Reply(URI lraId, List<Object> contexts) {
            super(lraId, contexts, null);
        }
    }
}
