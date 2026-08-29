package io.narayana.lra.contracts.http;

public class ParticipantLinks {
    public String compensateLink;
    public String completeLink;
    public String forgetLink;
    public String leaveLink;
    public String afterLink;
    public String statusLink;

    public ParticipantLinks() {
    }

    public ParticipantLinks(String compensateLink, String completeLink, String forgetLink,
            String leaveLink, String afterLink, String statusLink) {
        this.compensateLink = compensateLink;
        this.completeLink = completeLink;
        this.forgetLink = forgetLink;
        this.leaveLink = leaveLink;
        this.afterLink = afterLink;
        this.statusLink = statusLink;
    }
}
