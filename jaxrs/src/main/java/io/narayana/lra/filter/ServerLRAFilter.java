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
import io.narayana.lra.client.CallbackRegistrarHTTP;
import io.narayana.lra.client.LRAParticipantData;
import io.narayana.lra.client.NarayanaLRAClient;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipantRegistry;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.inject.Inject;
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
import java.util.regex.Pattern;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * JAX-RS container filter for LRA processing.
 * This is a thin wrapper that delegates all business logic to {@link LRARequestHandler}.
 *
 * <p>
 * The filter's responsibility is to:
 * <ol>
 * <li>Read HTTP headers and populate {@link LRARequestContext}</li>
 * <li>Delegate to {@link LRARequestHandler#filter(LRARequestContext)}</li>
 * <li>Write results back to HTTP headers</li>
 * <li>In the response filter, populate {@link LRAResponseContext} and delegate to {@link LRARequestHandler#responseFilter}</li>
 * </ol>
 */
@Provider
@ApplicationScoped
public class ServerLRAFilter implements ContainerRequestFilter, ContainerResponseFilter {
    private static final String CANCEL_ON_FAMILY_PROP = "CancelOnFamily";
    private static final String CANCEL_ON_PROP = "CancelOn";
    private static final String TERMINAL_LRA_PROP = "terminateLRA";
    private static final String SUSPENDED_LRA_PROP = "suspendLRA";
    private static final String CURRENT_LRA_PROP = "currentLRA";
    private static final String NEW_LRA_PROP = "newLRA";
    private static final String ABORT_WITH_PROP = "abortWith";
    private static final String PARTICIPANT_LINK_PROP = "compensatorURI";
    private static final Pattern START_END_QUOTES_PATTERN = Pattern.compile("^\"|\"$");

    @Context
    protected ResourceInfo resourceInfo;

    @Inject
    private LRAParticipantRegistry lraParticipantRegistry;

    @Inject
    LRAParticipantData data;

    @Inject
    @ConfigProperty(name = ENLIST_PARTICIPANT_CLIENT_MAX_RETRY, defaultValue = "3")
    int enlistMaxRetries;

    private NarayanaLRAClient lraClient;
    private LRARequestHandler requestHandler;

    @Override
    public void filter(ContainerRequestContext containerRequestContext) {
        Method method = resourceInfo.getResourceMethod();
        MultivaluedMap<String, String> headers = containerRequestContext.getHeaders();

        // Build transport-agnostic request context from HTTP request
        LRARequestContext ctx = new LRARequestContext();
        ctx.setResourceClass(resourceInfo.getResourceClass());
        ctx.setResourceMethod(method);
        ctx.setBaseUri(createUriPrefix(containerRequestContext));
        ctx.setRequestBaseUri(containerRequestContext.getUriInfo().getBaseUri());

        // Read incoming LRA from HTTP header
        if (headers.containsKey(LRA_HTTP_CONTEXT_HEADER)) {
            try {
                ctx.setIncomingLRA(new URI(Current.getLast(headers.get(LRA_HTTP_CONTEXT_HEADER))));
            } catch (URISyntaxException e) {
                String msg = String.format("header %s contains an invalid URL %s",
                        LRA_HTTP_CONTEXT_HEADER, Current.getLast(headers.get(LRA_HTTP_CONTEXT_HEADER)));
                containerRequestContext.abortWith(Response.status(Response.Status.PRECONDITION_FAILED.getStatusCode())
                        .entity(msg).build());
                return;
            }
        } else {
            // Check request properties (for nested/async scenarios)
            Object lraContext = containerRequestContext.getProperty(LRA_HTTP_CONTEXT_HEADER);
            if (lraContext != null) {
                ctx.setIncomingLRA((URI) lraContext);
            }
        }

        // Read Authorization header for @PropagateToken
        ctx.setAuthorizationHeader(containerRequestContext.getHeaderString(HttpHeaders.AUTHORIZATION));

        // Delegate to handler
        getLRARequestHandler().filter(ctx);

        // Handle abort
        if (ctx.isAborted()) {
            containerRequestContext.abortWith(
                    Response.status(ctx.getAbortStatusCode()).entity(ctx.getAbortMessage()).build());
            containerRequestContext.setProperty(ABORT_WITH_PROP, ctx.getAbortWith());
            return;
        }

        // If the handler signaled that context should be cleared (NOT_SUPPORTED, NEVER, etc.),
        // remove the LRA header from the request. This mirrors the original ServerLRAFilter's
        // Current.clearContext(headers) call.
        if (ctx.isClearContextHeaders()) {
            headers.remove(LRA_HTTP_CONTEXT_HEADER);
        }

        // Write results back to HTTP context
        // Store properties for the response filter
        if (ctx.getCancelOnFamily() != null) {
            containerRequestContext.setProperty(CANCEL_ON_FAMILY_PROP, ctx.getCancelOnFamily());
        }
        if (ctx.getCancelOn() != null) {
            containerRequestContext.setProperty(CANCEL_ON_PROP, ctx.getCancelOn());
        }
        if (ctx.getTerminalLRA() != null) {
            containerRequestContext.setProperty(TERMINAL_LRA_PROP, ctx.getTerminalLRA());
        }
        if (ctx.getSuspendedLRA() != null) {
            containerRequestContext.setProperty(SUSPENDED_LRA_PROP, ctx.getSuspendedLRA());
        }
        if (ctx.getNewLRA() != null) {
            containerRequestContext.setProperty(NEW_LRA_PROP, ctx.getNewLRA());
        }
        if (ctx.getCurrentLRA() != null) {
            containerRequestContext.setProperty(CURRENT_LRA_PROP, ctx.getCurrentLRA());
        }
        if (ctx.getCompensatorLink() != null) {
            containerRequestContext.setProperty(PARTICIPANT_LINK_PROP, ctx.getCompensatorLink());
        }

        // Write LRA context to HTTP headers (mirrors ServerLRAFilter's Current.updateLRAContext)
        if (!ctx.isClearContextHeaders() && ctx.getCurrentLRA() != null) {
            headers.putSingle(LRA_HTTP_CONTEXT_HEADER, ctx.getCurrentLRA().toASCIIString());
        }

        // Write parent context header
        if (ctx.getParentContextHeader() != null) {
            headers.putSingle(LRA_HTTP_PARENT_CONTEXT_HEADER, ctx.getParentContextHeader());
        }

        // Write recovery URL to response header
        if (ctx.getRecoveryUrl() != null) {
            headers.putSingle(LRA_HTTP_RECOVERY_HEADER,
                    START_END_QUOTES_PATTERN.matcher(ctx.getRecoveryUrl().toASCIIString()).replaceAll(""));
        }

        // Set user-defined participant data if the handler received it from the coordinator
        if (ctx.getPreviousParticipantData() != null) {
            setUserDefinedData(ctx.getPreviousParticipantData());
        }
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
        // Build request context from stored properties
        LRARequestContext requestCtx = new LRARequestContext();
        requestCtx.setCurrentLRA((URI) requestContext.getProperty(CURRENT_LRA_PROP));
        requestCtx.setSuspendedLRA((URI) requestContext.getProperty(SUSPENDED_LRA_PROP));
        requestCtx.setTerminalLRA((URI) requestContext.getProperty(TERMINAL_LRA_PROP));
        requestCtx.setAbortWith(requestContext.getProperty(ABORT_WITH_PROP));
        requestCtx.setCompensatorLink((String) requestContext.getProperty(PARTICIPANT_LINK_PROP));
        requestCtx.setResourceMethod(resourceInfo.getResourceMethod());
        requestCtx.setHeaders(requestContext.getHeaders());

        // Build response context
        LRAResponseContext responseCtx = new LRAResponseContext();
        responseCtx.setResponseStatus(responseContext.getStatus());
        responseCtx.setCancel(isJaxRsCancel(requestContext, responseContext));
        responseCtx.setUserData(getUserDefinedData());
        responseCtx.setCurrentLRA(requestCtx.getCurrentLRA());
        responseCtx.setSuspendedLRA(requestCtx.getSuspendedLRA());
        responseCtx.setToClose(requestCtx.getTerminalLRA());
        responseCtx.setCompensatorLink(requestCtx.getCompensatorLink());

        try {
            // Delegate to handler (handles cancel/close, header removal, async warning, failure messages)
            getLRARequestHandler().responseFilter(requestCtx, responseCtx);

            // Write results back
            if (responseCtx.getFailureMessage() != null) {
                responseContext.setEntity(responseCtx.getFailureMessage());
            }
        } finally {
            // Update response headers with current LRA context (mirrors old code's Current.updateLRAContext)
            // The handler captured the LRA context stack before clearing, so we can write it here
            List<Object> lraContexts = responseCtx.getLraContexts();
            if (lraContexts != null && !lraContexts.isEmpty()) {
                responseContext.getHeaders().put(LRA_HTTP_CONTEXT_HEADER, lraContexts);
            } else {
                responseContext.getHeaders().remove(LRA_HTTP_CONTEXT_HEADER);
            }
        }
    }

    private NarayanaLRAClient getLRAClient() {
        if (lraClient == null) {
            lraClient = new NarayanaLRAClient();
        }
        return lraClient;
    }

    private LRARequestHandler getLRARequestHandler() {
        if (requestHandler == null) {
            requestHandler = new LRARequestHandler(
                    getLRAClient(),
                    new CallbackRegistrarHTTP(),
                    lraParticipantRegistry,
                    enlistMaxRetries);
        }
        return requestHandler;
    }

    private URI createUriPrefix(ContainerRequestContext containerRequestContext) {
        return ConfigProvider.getConfig()
                .getOptionalValue(LRAConstants.NARAYANA_LRA_BASE_URI_PROPERTY_NAME, String.class)
                .map(s -> {
                    String userUri = s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
                    Class<?> resourceClass = resourceInfo.getResourceClass();
                    if (resourceClass.isAnnotationPresent(jakarta.ws.rs.Path.class)) {
                        String pathValue = resourceClass.getAnnotation(jakarta.ws.rs.Path.class).value();
                        return URI.create(userUri + (pathValue.startsWith("/") ? pathValue : "/" + pathValue));
                    }
                    return URI.create(userUri);
                })
                .orElseGet(() -> {
                    UriInfo uriInfo = containerRequestContext.getUriInfo();
                    List<String> matchedURIs = uriInfo.getMatchedURIs();
                    int matchedURI = (matchedURIs.size() > 1 ? 1 : 0);
                    return URI.create(uriInfo.getBaseUri() + matchedURIs.get(matchedURI));
                });
    }

    private boolean isJaxRsCancel(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
        int status = responseContext.getStatus();
        Response.Status.Family[] cancel0nFamily = (Response.Status.Family[]) requestContext.getProperty(CANCEL_ON_FAMILY_PROP);
        Response.Status[] cancel0n = (Response.Status[]) requestContext.getProperty(CANCEL_ON_PROP);

        if (cancel0nFamily != null) {
            if (Arrays.stream(cancel0nFamily).anyMatch(f -> Response.Status.Family.familyOf(status) == f)) {
                return true;
            }
        }

        if (cancel0n != null) {
            return Arrays.stream(cancel0n).anyMatch(f -> status == f.getStatusCode());
        }

        return false;
    }

    private String getUserDefinedData() {
        try {
            return data != null ? data.getData() : null;
        } catch (ContextNotActiveException e) {
            LRALogger.i18nLogger.warn_missingContexts("LRAParticipantData is not usable in this (probably async) context.");
        }
        return null;
    }

    private void setUserDefinedData(String userDefinedData) {
        try {
            if (data != null) {
                data.setData(userDefinedData);
            }
        } catch (ContextNotActiveException e) {
            LRALogger.i18nLogger.warn_missingContexts("LRAParticipantData is not usable in this (probably async) context.");
        }
    }
}
