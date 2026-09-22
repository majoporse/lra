package io.narayana.lra.contracts.http;

import io.narayana.lra.LRAData;
import io.narayana.lra.contracts.common.GetLRAInfoLRA;
import java.util.UUID;

public class GetLRAInfoLRAHttp {
    public static class Request extends GetLRAInfoLRA.Request {
        public Request() {
        }

        public Request(UUID lraId) {
            super(lraId);
        }
    }

    public static class Reply extends GetLRAInfoLRA.Reply {
        public Reply() {
        }

        public Reply(LRAData data) {
            super(data);
        }
    }
}
