package io.narayana.lra;

import jakarta.ws.rs.core.Link;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

public class LinkHelper {

    private static final String COMPENSATE_REL = "compensate";

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

    public static String cannonicalForm(String linkStr) throws URISyntaxException {
        if (!linkStr.contains(">;")) {
            return new URI(linkStr).toASCIIString();
        }

        SortedMap<String, String> lm = new TreeMap<>();
        Arrays.stream(linkStr.split(",")).forEach(link -> lm.put(Link.valueOf(link).getRel(), link));
        StringBuilder sb = new StringBuilder();

        lm.forEach((k, v) -> appendLink(sb, v));

        return sb.toString();
    }

    private static void appendLink(StringBuilder b, String value) {
        if (b.length() != 0) {
            b.append(',');
        }

        b.append(value);
    }
}
