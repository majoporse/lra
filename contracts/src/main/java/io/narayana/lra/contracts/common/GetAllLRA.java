package io.narayana.lra.contracts.common;

import io.narayana.lra.LRAData;
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
        public List<LRAData> data;

        public Reply() {
        }

        public Reply(List<LRAData> data) {
            this.data = data;
        }

    }
}
