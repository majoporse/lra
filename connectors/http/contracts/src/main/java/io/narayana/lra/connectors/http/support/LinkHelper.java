package io.narayana.lra.connectors.http.support;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Link-header helpers for the legacy participant path.
 * <p>
 * The parser is deliberately small and JAX-RS-free; it belongs to the legacy HTTP world.
 */
public class LinkHelper {

    private static final Pattern LINK_REL = Pattern.compile("<([^>]+)>[^,]*?;\\s*rel=\"?([^\";,\\s]+)\"?");

    private static final String COMPENSATE_REL = "compensate";

    public static String extractCompensator(String linkStr) throws URISyntaxException {
        for (String link : linkStr.split(",")) {
            String rel = rel(link);

            if (rel == null) {
                throw new URISyntaxException(link, "Invalid link");
            }

            if (COMPENSATE_REL.equals(rel)) {
                return new URI(uri(link)).toASCIIString();
            }
        }

        return linkStr;
    }

    public static String cannonicalForm(String linkStr) throws URISyntaxException {
        if (!linkStr.contains(">;")) {
            return new URI(linkStr).toASCIIString();
        }

        SortedMap<String, String> links = new TreeMap<>();

        for (String link : linkStr.split(",")) {
            String rel = rel(link);

            if (rel == null) {
                throw new URISyntaxException(link, "Invalid link");
            }

            links.put(rel, link);
        }

        StringBuilder sb = new StringBuilder();

        links.values().forEach(value -> {
            if (sb.length() != 0) {
                sb.append(',');
            }

            sb.append(value);
        });

        return sb.toString();
    }

    private static String rel(String link) {
        Matcher matcher = LINK_REL.matcher(link);

        return matcher.find() ? matcher.group(2) : null;
    }

    private static String uri(String link) {
        Matcher matcher = LINK_REL.matcher(link);

        return matcher.find() ? matcher.group(1) : null;
    }
}
