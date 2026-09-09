package io.narayana.lra.contracts.common;

import java.net.URI;
import java.util.List;

public class StartLRA {
    public static class Request {
        public String clientId;
        public Long timeout;
        public URI parentLRA;

        public Request() {
        }

        public Request(String clientId, Long timeout, URI parentLRA) {
            this.clientId = clientId;
            this.timeout = timeout;
            this.parentLRA = parentLRA;
        }
    }

    public static class Reply {
        public URI lraId;
        public List<Object> contexts;
        public String error;

        public Reply() {
        }

        public Reply(URI lraId, List<Object> contexts, String error) {
            this.lraId = lraId;

            this.contexts = contexts;
            this.error = error;
        }
    }
}
