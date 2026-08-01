/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.domain.model;

import io.narayana.lra.logging.LRALogger;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Static registry that holds {@link ParticipantNotifier} instances.
 * Provides {@link #getForEndpoint(String)} which checks the endpoint scheme
 * and returns the appropriate notifier.
 *
 * <p>
 * Notifiers are registered at application startup by the Quarkus
 * deployment module (see {@code NotifierInitializer}).
 * </p>
 *
 * <p>
 * This bridges CDI-managed notifiers to Narayana's non-CDI
 * {@link LRAParticipantRecord}.
 * </p>
 */
public final class ParticipantNotifierHolder {

    private static final List<ParticipantNotifier> notifiers = new CopyOnWriteArrayList<>();
    private static final HttpParticipantNotifier HTTP_DEFAULT = new HttpParticipantNotifier();

    private ParticipantNotifierHolder() {
    }

    /**
     * Register a notifier. Called at application startup.
     */
    public static void register(ParticipantNotifier notifier) {
        if (LRALogger.logger.isInfoEnabled()) {
            LRALogger.logger.infof("ParticipantNotifierHolder: registered %s", notifier.getClass().getName());
        }
        notifiers.add(notifier);
    }

    /**
     * Find the appropriate notifier for the given endpoint.
     * Iterates registered notifiers and returns the first one that supports
     * the endpoint scheme. Falls back to {@link HttpParticipantNotifier}
     * if no registered notifier matches.
     *
     * @param endpoint the callback endpoint (e.g., "http://..." or "kafka://...")
     * @return the appropriate notifier
     */
    public static ParticipantNotifier getForEndpoint(String endpoint) {
        for (ParticipantNotifier notifier : notifiers) {
            if (notifier.supports(endpoint)) {
                return notifier;
            }
        }
        // Fallback to HTTP notifier (backward compatible)
        return HTTP_DEFAULT;
    }

    /**
     * Get the HTTP notifier (for direct HTTP callbacks).
     */
    public static HttpParticipantNotifier getHttpNotifier() {
        for (ParticipantNotifier notifier : notifiers) {
            if (notifier instanceof HttpParticipantNotifier) {
                return (HttpParticipantNotifier) notifier;
            }
        }
        return HTTP_DEFAULT;
    }

    /**
     * Get the messaging notifier (for topic-based callbacks), if registered.
     *
     * @return the messaging notifier, or null if not registered
     */
    public static MessagingParticipantNotifier getMessagingNotifier() {
        for (ParticipantNotifier notifier : notifiers) {
            if (notifier instanceof MessagingParticipantNotifier) {
                return (MessagingParticipantNotifier) notifier;
            }
        }
        return null;
    }

    /**
     * Clear all registered notifiers (for testing).
     */
    public static void clear() {
        notifiers.clear();
    }
}
