package io.narayana.lra.contracts.common;

import java.net.URI;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class CloseLRA {
    public static class Request {
        public URI lraId;
        public String compensator;
        public String userData;

        public Request() {
        }

        public Request(URI lraId, String compensator, String userData) {
            this.lraId = lraId;
            this.compensator = compensator;
            this.userData = userData;
        }
    }

    public static class Reply {
        public LRAStatus status;

        public Reply() {
        }

        public Reply(LRAStatus status, String error) {
            this.status = status;
        }
    }
}
