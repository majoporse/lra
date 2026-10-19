package io.narayana.lra.contracts.http;

import io.narayana.lra.LRAData;
import io.narayana.lra.contracts.common.GetAllLRA;
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

        public Reply(List<LRAData> data) {
            super(data);
        }

    }
}
