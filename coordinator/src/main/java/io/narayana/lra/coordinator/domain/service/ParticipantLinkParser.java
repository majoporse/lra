package io.narayana.lra.coordinator.domain.service;

import static io.narayana.lra.LRAConstants.AFTER;

import io.narayana.lra.coordinator.domain.model.actions.HttpAction;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Link;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

@ApplicationScoped
public class ParticipantLinkParser {

    private static final String COMPENSATE_REL = "compensate";
    private static final String COMPLETE_REL = "complete";

    public ParticipantActions parse(String linkHeader) {
        ParticipantActions actions = new ParticipantActions();

        if (linkHeader == null || linkHeader.isEmpty()) {
            return actions;
        }

        if (linkHeader.startsWith("<")) {
            Arrays.stream(linkHeader.split(",")).forEach(linkStr -> {
                parseSingleLink(linkStr.trim(), actions);
            });
        } else {
            return parseSimple(linkHeader);
        }

        return actions;
    }

    public ParticipantActions parseSimple(String participantUrl) {
        ParticipantActions actions = new ParticipantActions();
        try {
            actions.compensateAction = new HttpAction(new URI(String.format("%s/compensate", participantUrl)));
            actions.completeAction = new HttpAction(new URI(String.format("%s/complete", participantUrl)));
            actions.statusAction = new HttpAction(new URI(participantUrl));
            actions.forgetAction = new HttpAction(new URI(participantUrl));
        } catch (URISyntaxException e) {
            LRALogger.logger.errorf("Invalid participant URL: %s (%s)", participantUrl, e.getMessage());
        }
        return actions;
    }

    private void parseSingleLink(String linkStr, ParticipantActions actions) {
        Link link = Link.valueOf(linkStr);
        String rel = link.getRel();

        try {
            URI uri = cannonicalURI(link.getUri());

            if (COMPENSATE_REL.equals(rel)) {
                actions.compensateAction = new HttpAction(uri);
            } else if (COMPLETE_REL.equals(rel)) {
                actions.completeAction = new HttpAction(uri);
            } else if ("status".equals(rel)) {
                actions.statusAction = new HttpAction(uri);
            } else if (AFTER.equals(rel)) {
                actions.afterAction = new HttpAction(uri);
            } else if ("forget".equals(rel)) {
                actions.forgetAction = new HttpAction(uri);
            } else if ("participant".equals(rel)) {
                actions.compensateAction = new HttpAction(new URI(uri.toASCIIString() + "/compensate"));
                actions.completeAction = new HttpAction(new URI(uri.toASCIIString() + "/complete"));
                actions.statusAction = new HttpAction(uri);
                actions.forgetAction = new HttpAction(uri);
            }
        } catch (URISyntaxException e) {
            LRALogger.logger.errorf("Invalid URI in link: %s (%s)", linkStr, e.getMessage());
        }
    }

    public static String cannonicalForm(String linkStr) throws URISyntaxException {
        if (!linkStr.contains(">;")) {
            return new URI(linkStr).toASCIIString();
        }

        SortedMap<String, String> lm = new TreeMap<>();
        Arrays.stream(linkStr.split(",")).forEach(link -> lm.put(Link.valueOf(link).getRel(), link));
        StringBuilder sb = new StringBuilder();

        lm.forEach((k, v) -> {
            if (sb.length() != 0) {
                sb.append(',');
            }
            sb.append(v);
        });

        return sb.toString();
    }

    public static String extractCompensator(String linkStr) throws URISyntaxException {
        for (String lnk : linkStr.split(",")) {
            Link link;
            try {
                link = Link.valueOf(lnk);
            } catch (IllegalArgumentException e) {
                throw new URISyntaxException(lnk, e.getMessage());
            }

            if (COMPENSATE_REL.equals(link.getRel())) {
                return cannonicalForm(link.getUri().toString());
            }
        }
        return linkStr;
    }

    private static URI cannonicalURI(URI uri) throws URISyntaxException {
        return new URI(uri.getScheme(),
                uri.getUserInfo(),
                uri.getHost(),
                uri.getPort(),
                uri.getPath().replaceAll("//", "/"),
                uri.getQuery(), uri.getFragment());
    }
}
