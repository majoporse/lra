/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.jaxrs;

import static io.narayana.lra.LRAConstants.ENLIST_PARTICIPANT_CLIENT_MAX_RETRY;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;

import io.narayana.lra.Current;
import io.narayana.lra.api.Outcome;
import io.narayana.lra.connectors.common.client.invocation.LRAParticipantData;
import io.narayana.lra.connectors.common.client.invocation.LraInvocation;
import io.narayana.lra.connectors.common.client.invocation.ServerLRAContext;
import io.narayana.lra.connectors.common.client.participant.LRAParticipantRegistry;
import io.narayana.lra.connectors.common.client.participant.ParticipantCallbackAddressResolver;
import io.narayana.lra.connectors.common.client.participant.protocol.ParticipantProtocolRegistry;
import io.narayana.lra.connectors.http.client.NarayanaLRAClient;
import io.narayana.lra.connectors.http.client.lifecycle.MicroProfileLRAAnnotationResolver;
import io.narayana.lra.connectors.http.client.lifecycle.ServerLRAProcessor;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import java.net.URI;
import java.util.List;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * The JAX-RS side of the LRA lifecycle: it completes the invocation the interceptor started, once
 * the final HTTP status is known.
 * <p>
 * It contains no annotation handling and no lifecycle state machine. It reads the state the
 * interceptor stored on the request, translates the status into a cancel decision using the
 * {@code @LRA} rules the interceptor already resolved, publishes the effective LRA on the response
 * and restores the thread-local state. All decisions live in {@link ServerLRAProcessor}.
 */
@Provider
@ApplicationScoped
public class LraResponseFilter implements ContainerResponseFilter {

    static final String PROPAGATION_PROPERTY = LraResponseFilter.class.getName() + ".propagation";

    @Inject
    private Instance<LRAParticipantRegistry> lraParticipantRegistry;

    @Inject
    private ParticipantProtocolRegistry protocolRegistry;

    @Inject
    private LRAParticipantData data;

    @Inject
    @ConfigProperty(name = ENLIST_PARTICIPANT_CLIENT_MAX_RETRY, defaultValue = "3")
    int enlistMaxRetries;

    private volatile ServerLRAProcessor processor;

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
        if (requestContext.getProperty(PROPAGATION_PROPERTY) != null) {
            // the request filter pushed the incoming LRA for an unannotated resource
            requestContext.removeProperty(PROPAGATION_PROPERTY);
            Current.popAll();
            Current.clearAuthToken();
            return;
        }

        Object property = requestContext.getProperty(LraInvocation.PROPERTY);

        if (!(property instanceof LraInvocation invocation)) {
            // the interceptor did not manage this invocation
            return;
        }

        ServerLRAContext context = invocation.getContext();

        try {
            boolean cancel = isCancel(context, responseContext.getStatus());
            getProcessor().processResponse(context, cancel);

            if (context.getProgress() != null) {
                String failureMessage = getProcessor().processFailures(context);

                if (failureMessage != null) {
                    responseContext.setEntity(failureMessage);
                    LRALogger.logger.warn(failureMessage);
                }
            }

            updateResponseHeader(context, responseContext);
        } finally {
            // the HTTP request boundary ends every context this request created
            Current.popAll();
            Current.removeActiveLRACache(context.getCurrentLRA());
            Current.clearAuthToken();
            requestContext.removeProperty(LraInvocation.PROPERTY);
        }
    }

    private static boolean isCancel(ServerLRAContext context, int status) {
        Set<Outcome> cancelOnFamily = context.getCancelOnFamily();
        Set<Integer> cancelOn = context.getCancelOn();

        if (cancelOnFamily != null && cancelOnFamily.contains(outcomeOf(status))) {
            return true;
        }

        return cancelOn != null && cancelOn.contains(status);
    }

    private static Outcome outcomeOf(int status) {
        return switch (Response.Status.Family.familyOf(status)) {
            case INFORMATIONAL -> Outcome.INFORMATIONAL;
            case SUCCESSFUL -> Outcome.SUCCESS;
            case REDIRECTION -> Outcome.REDIRECTION;
            case CLIENT_ERROR -> Outcome.CLIENT_ERROR;
            case SERVER_ERROR -> Outcome.SERVER_ERROR;
            case OTHER -> Outcome.OTHER;
        };
    }

    private static void updateResponseHeader(ServerLRAContext context, ContainerResponseContext responseContext) {
        // a nested/requires-new/not-supported invocation suspended the caller's LRA: the response
        // advertises the LRA the caller is still in, never the one this invocation ended
        URI lraId = context.isSuspendedLRA() && context.getIncomingLRA() != null
                ? context.getIncomingLRA()
                : context.getCurrentLRA();

        if (lraId != null) {
            responseContext.getHeaders().put(LRA_HTTP_CONTEXT_HEADER, List.of(lraId));
        } else {
            responseContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
        }
    }

    private ServerLRAProcessor getProcessor() {
        if (processor == null) {
            synchronized (this) {
                if (processor == null) {
                    LRAParticipantRegistry registry = lraParticipantRegistry != null
                            && lraParticipantRegistry.isResolvable()
                                    ? lraParticipantRegistry.get()
                                    : null;
                    // the response can be produced on a thread where the request-scoped client is
                    // no longer active, so the adapter uses a directly constructed HTTP client
                    processor = new ServerLRAProcessor(registry, data, enlistMaxRetries,
                            new MicroProfileLRAAnnotationResolver(), new NarayanaLRAClient(), callbackResolver());
                }
            }
        }

        return processor;
    }

    private ParticipantCallbackAddressResolver callbackResolver() {
        return protocolRegistry != null ? protocolRegistry.resolver() : null;
    }
}
