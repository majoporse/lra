/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter;

import static io.narayana.lra.LRAConstants.ENLIST_PARTICIPANT_CLIENT_MAX_RETRY;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_PARENT_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import io.narayana.lra.Current;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.connectors.common.client.LRAClient;
import io.narayana.lra.connectors.common.client.LRAParticipantData;
import io.narayana.lra.connectors.common.client.ParticipantCallbackAddressResolver;
import io.narayana.lra.connectors.common.client.internal.ParticipantProtocolRegistry;
import io.narayana.lra.connectors.common.client.participant.LRAParticipantRegistry;
import io.narayana.lra.connectors.http.client.NarayanaLRAClient;
import io.narayana.lra.connectors.http.client.legacy.LegacyLRAParticipant;
import io.narayana.lra.filter.core.ServerLRAContext;
import io.narayana.lra.filter.core.ServerLRAProcessor;
import io.narayana.lra.filter.resolver.MicroProfileLRAAnnotationResolver;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.Provider;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Provider
@ApplicationScoped
public class ServerLRAFilter implements ContainerRequestFilter, ContainerResponseFilter {
    private static final String PARTICIPANT_LINK_PROP = "compensatorURI";

    @Context
    protected ResourceInfo resourceInfo;

    @Inject
    private Instance<LRAParticipantRegistry> lraParticipantRegistry;

    @Inject
    private ParticipantProtocolRegistry protocolRegistry;

    @Inject
    LRAParticipantData data;

    @Inject
    @ConfigProperty(name = ENLIST_PARTICIPANT_CLIENT_MAX_RETRY, defaultValue = "3")
    int enlistMaxRetries;

    private ServerLRAProcessor processor;

    @Override
    public void filter(ContainerRequestContext containerRequestContext) {
        if (LRAConstants.isInterceptorMode()) {
            // LRA processing is delegated to the CDI interceptor (@LRA.Transaction beans)
            return;
        }

        // Note that this filter uses abortWith instead of throwing exceptions on encountering exceptional
        // conditions. This facilitates async because filters for asynchronous JAX-RS methods are
        // not allowed to throw exceptions.
        Method method = resourceInfo.getResourceMethod();
        ServerLRAContext context = new ServerLRAContext(method, resourceInfo.getResourceClass());

        // make the context available to the response filter
        containerRequestContext.setProperty(ServerLRAContext.PROPERTY_NAME, context);

        if (populateContext(containerRequestContext, context)) {
            getProcessor().processRequest(context);
        }

        applyRequestContext(containerRequestContext, context);
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
        if (LRAConstants.isInterceptorMode()) {
            // the interceptor owns the LRA lifecycle (and the thread-local cleanup); do nothing
            return;
        }

        // a request is leaving the container so clear any context on the thread and fix up the LRA response header
        ServerLRAContext context = (ServerLRAContext) requestContext.getProperty(ServerLRAContext.PROPERTY_NAME);

        if (context == null) {
            // the request filter did not run (or did not produce a context); nothing to report
            updateLRAContext(responseContext);
            Current.popAll();
            Current.clearAuthToken();
            return;
        }

        boolean isCancel = isJaxRsCancel(context, responseContext.getStatus());

        try {
            getProcessor().processResponse(context, isCancel);

            invalidateRequestContextHeader(requestContext, context);

            if (responseContext.getStatus() == Response.Status.OK.getStatusCode()
                    && resourceInfo.getResourceMethod() != null
                    && LegacyLRAParticipant.isAsyncCompletion(resourceInfo.getResourceMethod())) {
                LRALogger.i18nLogger.warn_lraParticipantqForAsync(
                        resourceInfo.getResourceMethod().getDeclaringClass().getName(),
                        resourceInfo.getResourceMethod().getName(),
                        Response.Status.ACCEPTED.getStatusCode(),
                        Response.Status.OK.getStatusCode());
            }

            /*
             * report any failed steps (ie if progress contains any failures) to the caller.
             * If either filter encountered a failure they may have completed partial actions, and
             * we need tell the caller which steps failed and which ones succeeded.
             */
            if (context.getProgress() != null) {
                String failureMessage = getProcessor().processFailures(context);

                if (failureMessage != null) {
                    responseContext.setEntity(failureMessage);

                    LRALogger.logger.warn(failureMessage); // any other failure(s) will already have been logged
                }
            }
        } finally {
            if (context.isSuspendedLRA() && context.getIncomingLRA() != null) {
                Current.push(context.getIncomingLRA(), null);
            }

            updateLRAContext(responseContext);

            Current.popAll();
            Current.removeActiveLRACache(context.getCurrentLRA());
            Current.clearAuthToken();
        }
    }

    // translate the transport state of the request into the context handed to the processor
    private boolean populateContext(ContainerRequestContext requestContext, ServerLRAContext context) {
        MultivaluedMap<String, String> headers = requestContext.getHeaders();

        context.setAuthHeader(requestContext.getHeaderString(HttpHeaders.AUTHORIZATION));

        if (headers.containsKey(LRA_HTTP_CONTEXT_HEADER)) {
            String header = Current.getLast(headers.get(LRA_HTTP_CONTEXT_HEADER));

            try {
                context.setIncomingLRA(new URI(header));
            } catch (URISyntaxException e) {
                String msg = String.format("header %s contains an invalid URL %s", LRA_HTTP_CONTEXT_HEADER, header);

                Method method = context.getMethod();
                LRALogger.i18nLogger.warn_lraFilterContainerRequest(msg,
                        method.getDeclaringClass().getName() + "#" + method.getName(), "context");

                context.setAborted(true);
                context.setAbortStatusCode(Response.Status.PRECONDITION_FAILED.getStatusCode());
                context.setAbortMessage(msg);

                return false;
            }
        } else {
            Object lraContext = requestContext.getProperty(LRA_HTTP_CONTEXT_HEADER);

            if (lraContext != null) {
                context.setContextPropertyLRA((URI) lraContext);
            }
        }

        context.setUriPrefix(() -> createUriPrefix(requestContext, resourceInfo.getResourceClass()));
        context.setBaseUri(requestContext.getUriInfo().getBaseUri());

        return true;
    }

    // translate the decisions taken by the processor into the request (headers and properties)
    private void applyRequestContext(ContainerRequestContext requestContext, ServerLRAContext context) {
        MultivaluedMap<String, String> headers = requestContext.getHeaders();

        if (context.isParentHeaderHandled()) {
            if (context.getParentLRA() != null) {
                headers.putSingle(LRA_HTTP_PARENT_CONTEXT_HEADER, context.getParentLRA().toASCIIString());
            } else {
                headers.remove(LRA_HTTP_PARENT_CONTEXT_HEADER);
            }
        }

        if (context.isClearContextHeader()) {
            headers.remove(LRA_HTTP_CONTEXT_HEADER);
        } else if (context.getCurrentLRA() != null) {
            headers.putSingle(LRA_HTTP_CONTEXT_HEADER, context.getCurrentLRA().toString());
        }

        if (context.getRecoveryUrl() != null) {
            headers.putSingle(LRA_HTTP_RECOVERY_HEADER, context.getRecoveryUrl());
        }

        if (context.getCallbacks() != null) {
            // store the registration link in case the participant wants to associate data with the enlistment in the LRA
            requestContext.setProperty(PARTICIPANT_LINK_PROP, context.getCallbacks());
        }

        if (context.isAborted()) {
            // the response filter will set the entity body
            requestContext.abortWith(Response.status(context.getAbortStatusCode())
                    .entity(context.getAbortMessage()).build());
        }
    }

    // the response filter invalidates the caller's context header once the LRA has been finished
    private void invalidateRequestContextHeader(ContainerRequestContext requestContext, ServerLRAContext context) {
        if (context.isCurrentLRACancelled()
                && context.getCurrentLRA().toASCIIString().equals(
                        Current.getLast(requestContext.getHeaders().get(LRA_HTTP_CONTEXT_HEADER)))) {
            // the callers context was ended so invalidate it
            requestContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
        }

        if (context.isEndLRAClosed()) {
            requestContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
        }
    }

    private boolean isJaxRsCancel(ServerLRAContext context, int status) {
        Response.Status.Family[] cancelOnFamily = context.getCancelOnFamily();
        Response.Status[] cancelOn = context.getCancelOn();

        if (cancelOnFamily != null) {
            if (Arrays.stream(cancelOnFamily).anyMatch(f -> Response.Status.Family.familyOf(status) == f)) {
                return true;
            }
        }

        if (cancelOn != null) {
            return Arrays.stream(cancelOn).anyMatch(f -> status == f.getStatusCode());
        }

        return false;
    }

    private String createUriPrefix(ContainerRequestContext containerRequestContext, Class<?> resourceClass) {
        return LRAConstants.getBaseUri()
                .map(s -> {
                    String userUri = s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
                    if (resourceClass.isAnnotationPresent(Path.class)) {
                        String pathValue = resourceClass.getAnnotation(Path.class).value();
                        return userUri + (pathValue.startsWith("/") ? pathValue : "/" + pathValue);
                    }
                    return userUri;
                })
                .orElseGet(() -> {
                    UriInfo uriInfo = containerRequestContext.getUriInfo();

                    /*
                     * Calculate which path to prepend to the LRA participant methods. If there is more than one matching URI
                     * then the second matched URI comes from either the class level Path annotation or from a sub-resource
                     * locator.
                     * In both cases the second matched URI can be used as a prefix for the LRA participant URIs:
                     */
                    List<String> matchedURIs = uriInfo.getMatchedURIs();
                    int matchedURI = (matchedURIs.size() > 1 ? 1 : 0);
                    return uriInfo.getBaseUri() + matchedURIs.get(matchedURI);
                });
    }

    private ServerLRAProcessor getProcessor() {
        if (processor == null) {
            // no need to lock
            LRAParticipantRegistry legacyRegistry = lraParticipantRegistry != null
                    && lraParticipantRegistry.isResolvable()
                            ? lraParticipantRegistry.get()
                            : null;
            processor = new ServerLRAProcessor(legacyRegistry, data, enlistMaxRetries,
                    new MicroProfileLRAAnnotationResolver(), lraClient(), callbackResolver());
        }

        return processor;
    }

    private static void updateLRAContext(ContainerResponseContext responseContext) {
        java.net.URI lraId = Current.peek();

        if (lraId != null) {
            responseContext.getHeaders().put(LRA_HTTP_CONTEXT_HEADER, Current.getContexts());
        } else {
            responseContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
        }
    }

    private LRAClient lraClient() {
        // This filter is application-scoped and its processor is shared across requests. A
        // request-scoped CDI client proxy cannot be used when RESTEasy resumes an async response
        // on a non-request thread. The legacy filter is HTTP-only, so use a directly constructed
        // HTTP client (which also covers the non-CDI RESTEasy/Undertow tests).
        return new NarayanaLRAClient();
    }

    private ParticipantCallbackAddressResolver callbackResolver() {
        return protocolRegistry != null ? protocolRegistry.resolver() : null;
    }
}
