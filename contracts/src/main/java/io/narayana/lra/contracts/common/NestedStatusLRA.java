package io.narayana.lra.contracts.common;

import java.net.URI;

public class NestedStatusLRA {
    public static class Request {
        public URI nestedLraId;

        public Request() {
        }

        public Request(URI nestedLraId) {
            this.nestedLraId = nestedLraId;
        }
    }

    public static class Reply {

        public Reply() {
        }
    }
}
