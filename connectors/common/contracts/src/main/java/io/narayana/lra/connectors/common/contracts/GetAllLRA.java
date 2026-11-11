package io.narayana.lra.connectors.common.contracts;

import java.util.List;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class GetAllLRA {
    public static class Request {
        public LRAStatus status;

        public Request() {
        }

        public Request(LRAStatus status) {
            this.status = status;
        }
    }

    public static class Reply {
        public List<LRAInfo> data;

        public Reply() {
        }

        public Reply(List<LRAInfo> data) {
            this.data = data;
        }

    }
}
