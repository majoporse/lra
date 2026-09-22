package io.narayana.lra.contracts.common;

import io.narayana.lra.callbacks.ParticipantCallbacks;
import java.util.UUID;

public class JoinLRA {
    public static class Request {
        public UUID lraId;
        public Long timeLimit;
        public String partId;
        public String userData;

        public ParticipantCallbacks callbacks;

        public Request() {
        }

        public Request(UUID lraId, Long timeLimit,
                ParticipantCallbacks callbacks,
                String userData, String partId) {
            this.lraId = lraId;
            this.timeLimit = timeLimit;
            this.callbacks = callbacks;
            this.partId = partId;
            this.userData = userData;
        }
    }

    public static class Reply {
        public String recoveryUrl;
        public String previousCompensatorData;

        public Reply() {
        }

        public Reply(String recoveryUrl, String previousCompensatorData, String error) {
            this.recoveryUrl = recoveryUrl;
            this.previousCompensatorData = previousCompensatorData;
        }
    }
}
