/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.support;

import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.http.address.LegacyHttpAddress;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the HTTP Link header representation of a legacy {@code @Path} participant.
 */
public final class LinkHeaderParser {

    private static final Pattern LINK_REL_PATTERN = Pattern.compile("<([^>]+)>[^,]*?;\\s*rel=\"?([^\";,\\s]+)\"?");

    private LinkHeaderParser() {
    }

    public static ParticipantCallbacks parse(String linkHeader) {
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
                case "compensate" -> callbacks.compensateCallback = LegacyHttpAddress.of(
                        CallbackType.COMPENSATE, uri);
                case "complete" -> callbacks.completeCallback = LegacyHttpAddress.of(
                        CallbackType.COMPLETE, uri);
                case "status" -> callbacks.statusCallback = LegacyHttpAddress.of(CallbackType.STATUS,
                        uri);
                case "forget" -> callbacks.forgetCallback = LegacyHttpAddress.of(CallbackType.FORGET,
                        uri);
                case "after" -> callbacks.afterCallback = LegacyHttpAddress.of(CallbackType.AFTER, uri);
                case "participant" -> {
                    callbacks.compensateCallback = LegacyHttpAddress.of(CallbackType.COMPENSATE,
                            URI.create(uri.toASCIIString() + "/compensate"));
                    callbacks.completeCallback = LegacyHttpAddress.of(CallbackType.COMPLETE,
                            URI.create(uri.toASCIIString() + "/complete"));
                    callbacks.statusCallback = LegacyHttpAddress.of(CallbackType.STATUS, uri);
                    callbacks.forgetCallback = LegacyHttpAddress.of(CallbackType.FORGET, uri);
                }
                default -> {
                    /* Ignore unrecognized relations */
                }
            }
        }

        return callbacks;
    }
}
