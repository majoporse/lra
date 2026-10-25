package io.narayana.lra.connectors.http.contracts;

import io.narayana.lra.connectors.common.contracts.GetAllLRA;
import io.narayana.lra.connectors.common.contracts.LRAInfo;
import java.util.List;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class GetAllLRAHttp {
    public static class Request extends GetAllLRA.Request {
        public Request() {
        }

        public Request(LRAStatus status) {
            super(status);
        }

    }

    public static class Reply extends GetAllLRA.Reply {
        public Reply() {
        }

        public Reply(List<LRAInfo> data) {
            super(data);
        }

    }
}
