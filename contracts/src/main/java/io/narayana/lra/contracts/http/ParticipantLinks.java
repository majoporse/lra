package io.narayana.lra.contracts.http;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParticipantLinks {
    public URI compensateLink;
    public URI completeLink;
    public URI forgetLink;
    public URI leaveLink;
    public URI afterLink;
    public URI statusLink;

    public ParticipantLinks() {
    }

    public ParticipantLinks(URI compensateLink, URI completeLink, URI forgetLink,
            URI leaveLink, URI afterLink, URI statusLink) {
        this.compensateLink = compensateLink;
        this.completeLink = completeLink;
        this.forgetLink = forgetLink;
        this.leaveLink = leaveLink;
        this.afterLink = afterLink;
        this.statusLink = statusLink;
    }

    public static ParticipantLinks fromLinkString(String headerValue) {
        Pattern LINK_REL_PATTERN = Pattern.compile("<([^>]+)>[^,]*?;\\s*rel=\"?([^\";,\\s]+)\"?");

        if (headerValue == null || headerValue.isBlank()) {
            return new ParticipantLinks();
        }

        URI compensate = null;
        URI complete = null;
        URI forget = null;
        URI leave = null;
        URI after = null;
        URI status = null;

        Matcher matcher = LINK_REL_PATTERN.matcher(headerValue);

        while (matcher.find()) {
            String uriString = matcher.group(1);
            String rel = matcher.group(2).toLowerCase();
            URI uri = (uriString == null) ? null : URI.create(uriString);

            switch (rel) {
                case "compensate" -> compensate = uri;
                case "complete" -> complete = uri;
                case "forget" -> forget = uri;
                case "leave" -> leave = uri;
                case "after" -> after = uri;
                case "status" -> status = uri;
                case "participant" -> {
                    compensate = URI.create(uri.toASCIIString() + "/compensate");
                    complete = URI.create(uri.toASCIIString() + "/complete");
                    status = forget = uri;
                }
                default -> {
                    /* Ignore unrecognized relations */ }
            }
        }

        return new ParticipantLinks(compensate, complete, forget, leave, after, status);
    }
}
