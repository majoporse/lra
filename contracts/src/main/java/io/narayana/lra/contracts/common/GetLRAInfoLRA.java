package io.narayana.lra.contracts.common;

import io.narayana.lra.LRAData;
import java.net.URI;

public class GetLRAInfoLRA {
    public static class Request {
        public URI lraId;

        public Request() {
        }

        public Request(URI lraId) {
            this.lraId = lraId;
        }
    }

    public static class Reply {
        public LRAData data;

        public Reply() {
        }

        public Reply(LRAData data) {
            this.data = data;
        }
    }

}
