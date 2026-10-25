package io.narayana.lra.connectors.common.contracts;

import java.util.UUID;

public class GetLRAInfoLRA {
    public static class Request {
        public UUID lraId;

        public Request() {
        }

        public Request(UUID lraId) {
            this.lraId = lraId;
        }
    }

    public static class Reply {
        public LRAInfo data;

        public Reply() {
        }

        public Reply(LRAInfo data) {
            this.data = data;
        }
    }

}
