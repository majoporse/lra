package io.narayana.lra.contracts.http;

public class NestedStatusLRAHttp {
    public static class Request {
        public String nestedLraId;

        public Request() {
        }

        public Request(String nestedLraId) {
            this.nestedLraId = nestedLraId;
        }
    }

    public static class Reply {
        public String status;

        public Reply() {
        }

        public Reply(String status) {
            this.status = status;
        }
    }
}
