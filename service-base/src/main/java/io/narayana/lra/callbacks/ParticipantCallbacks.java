package io.narayana.lra.callbacks;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParticipantCallbacks {
    private static final Pattern LINK_REL_PATTERN = Pattern.compile("<([^>]+)>[^,]*?;\\s*rel=\"?([^\";,\\s]+)\"?");

    public LRACallback compensateCallback;
    public LRACallback completeCallback;
    public LRACallback statusCallback;
    public LRACallback forgetCallback;
    public LRACallback afterCallback;

    public boolean hasAnyCallback() {
        return compensateCallback != null || completeCallback != null || statusCallback != null
                || forgetCallback != null || afterCallback != null;
    }

    public boolean hasCompensateOrComplete() {
        return compensateCallback != null || completeCallback != null;
    }

    // parse the HTTP Link header representation of a participant into callback objects
    public static ParticipantCallbacks fromLinkString(String linkHeader) {
        ParticipantCallbacks callbacks = new ParticipantCallbacks();

        if (linkHeader == null || linkHeader.isBlank()) {
            return callbacks;
        }

        Matcher matcher = LINK_REL_PATTERN.matcher(linkHeader);

        while (matcher.find()) {
            String uriString = matcher.group(1);
            String rel = matcher.group(2).toLowerCase();
            URI uri = URI.create(uriString);

            switch (rel) {
                case "compensate" -> callbacks.compensateCallback = HttpCallback.compensateCallback(uri);
                case "complete" -> callbacks.completeCallback = HttpCallback.completeCallback(uri);
                case "status" -> callbacks.statusCallback = HttpCallback.statusCallback(uri);
                case "forget" -> callbacks.forgetCallback = HttpCallback.forgetCallback(uri);
                case "after" -> callbacks.afterCallback = HttpCallback.afterCallback(uri);
                case "participant" -> {
                    callbacks.compensateCallback = HttpCallback.compensateCallback(
                            URI.create(uri.toASCIIString() + "/compensate"));
                    callbacks.completeCallback = HttpCallback.completeCallback(
                            URI.create(uri.toASCIIString() + "/complete"));
                    callbacks.statusCallback = HttpCallback.statusCallback(uri);
                    callbacks.forgetCallback = HttpCallback.forgetCallback(uri);
                }
                default -> {
                    /* Ignore unrecognized relations */
                }
            }
        }

        return callbacks;
    }
}
