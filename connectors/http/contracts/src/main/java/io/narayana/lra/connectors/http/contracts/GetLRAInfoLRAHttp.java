package io.narayana.lra.connectors.http.contracts;

import io.narayana.lra.connectors.common.contracts.GetLRAInfoLRA;
import io.narayana.lra.connectors.common.contracts.LRAInfo;
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

        public Reply(LRAInfo data) {
            super(data);
        }
    }
}
