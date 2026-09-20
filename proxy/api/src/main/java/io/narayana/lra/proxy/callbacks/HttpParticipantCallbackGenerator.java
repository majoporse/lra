/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.proxy.callbacks;

import static io.narayana.lra.LRAConstants.AFTER;
import static io.narayana.lra.LRAConstants.COMPENSATE;
import static io.narayana.lra.LRAConstants.COMPLETE;
import static io.narayana.lra.LRAConstants.FORGET;
import static io.narayana.lra.LRAConstants.NARAYANA_LRA_BASE_URI_PROPERTY_NAME;
import static io.narayana.lra.LRAConstants.STATUS;

import io.narayana.lra.callbacks.HttpCallback;
import io.narayana.lra.callbacks.ParticipantCallbacks;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipant;
import io.narayana.lra.client.internal.proxy.nonjaxrs.listeners.LRAParticipantResource;
import jakarta.ws.rs.core.UriBuilder;
import java.net.URI;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Generates HTTP callbacks that target the {@link LRAParticipantResource} endpoints of the
 * participant. The base URI is resolved from the {@code narayana.lra.base-uri} configuration
 * property rather than from the current request context.
 */
public class HttpParticipantCallbackGenerator implements ParticipantCallbackGenerator {

    static final String DEFAULT_BASE_URI = "http://localhost:8080";

    @Override
    public ParticipantCallbacks getCallbacks(LRAParticipant participant, ParticipantCallbacks existing) {
        String baseUri = ConfigProvider.getConfig()
                .getOptionalValue(NARAYANA_LRA_BASE_URI_PROPERTY_NAME, String.class)
                .orElse(DEFAULT_BASE_URI);

        String resourceBase = UriBuilder.fromUri(baseUri)
                .path(LRAParticipantResource.RESOURCE_PATH)
                .path(participant.getJavaClass().getName())
                .build()
                .toASCIIString();

        ParticipantCallbacks callbacks = new ParticipantCallbacks();

        if (existing.completeCallback != null) {
            callbacks.completeCallback = existing.completeCallback;
        } else if (participant.hasCompleteMethod()) {
            callbacks.completeCallback = HttpCallback.completeCallback(URI.create(getURI(resourceBase, COMPLETE)));
        }

        if (existing.compensateCallback != null) {
            callbacks.compensateCallback = existing.compensateCallback;
        } else if (participant.hasCompensateMethod()) {
            callbacks.compensateCallback = HttpCallback.compensateCallback(URI.create(getURI(resourceBase, COMPENSATE)));
        }

        if (existing.statusCallback != null) {
            callbacks.statusCallback = existing.statusCallback;
        } else if (participant.hasStatusMethod()) {
            callbacks.statusCallback = HttpCallback.statusCallback(URI.create(getURI(resourceBase, STATUS)));
        }

        if (existing.forgetCallback != null) {
            callbacks.forgetCallback = existing.forgetCallback;
        } else if (participant.hasForgetMethod()) {
            callbacks.forgetCallback = HttpCallback.forgetCallback(URI.create(getURI(resourceBase, FORGET)));
        }

        if (existing.afterCallback != null) {
            callbacks.afterCallback = existing.afterCallback;
        } else if (participant.hasAfterLRAMethod()) {
            callbacks.afterCallback = HttpCallback.afterCallback(URI.create(getURI(resourceBase, AFTER)));
        }

        return callbacks;
    }

    private static String getURI(String baseURI, String path) {
        return String.format("%s/%s", baseURI, path);
    }
}