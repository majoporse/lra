package io.narayana.lra.contracts.http;

public class NestedForgetLRAHttp {
    public static class Request {
        public String nestedLraId;

        public Request() {
        }

        public Request(String nestedLraId) {
            this.nestedLraId = nestedLraId;
        }
    }

    public static class Reply {
        public Reply() {
        }
    }
}
