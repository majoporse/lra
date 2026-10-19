package io.narayana.lra.contracts.common;

import io.narayana.lra.LRAData;
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
        public LRAData data;

        public Reply() {
        }

        public Reply(LRAData data) {
            this.data = data;
        }
    }

}
