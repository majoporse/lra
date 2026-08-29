package io.narayana.lra.contracts.http;

import io.narayana.lra.contracts.common.JoinLRA;
import java.net.URI;

public class JoinLRAHttp {
    public static class Request extends JoinLRA.Request {
        public ParticipantLinks participantLinks;
        public String compensatorLink;

        public Request() {
        }

        public Request(
                URI lraId,
                Long timeLimit,
                ParticipantLinks participantLinks,
                String compensatorURL,
                String userData,
                String partId,
                String compensatorLink) {
            super(lraId, timeLimit,
                    participantLinks,
                    compensatorURL,
                    userData,
                    partId);
            this.participantLinks = participantLinks;
            this.compensatorLink = compensatorLink;
        }
    }

    public static class Reply extends JoinLRA.Reply {
        public Reply() {
        }

        public Reply(String recoveryUrl, String previousCompensatorData) {
            super(recoveryUrl, previousCompensatorData, null);
        }
    }
}
