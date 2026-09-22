/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client.internal.proxy.nonjaxrs.listeners;

import static io.narayana.lra.LRAConstants.AFTER;
import static io.narayana.lra.LRAConstants.COMPENSATE;
import static io.narayana.lra.LRAConstants.COMPLETE;
import static io.narayana.lra.LRAConstants.FORGET;
import static io.narayana.lra.LRAConstants.STATUS;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_ENDED_CONTEXT_HEADER;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_PARENT_CONTEXT_HEADER;

import io.narayana.lra.callbacks.CallbackResult;
import io.narayana.lra.callbacks.CallbackStatus;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipant;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipantRegistry;
import io.narayana.lra.proxy.logging.LRAProxyLogger;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.Status;

@ApplicationScoped
@Path(LRAParticipantResource.RESOURCE_PATH)
@IfBuildProperty(name = "quarkus.lra.client.protocol", stringValue = "http")
public class LRAParticipantResource {

    public static final String RESOURCE_PATH = "lra-participant-proxy";

    @Inject
    private LRAParticipantRegistry lraParticipantRegistry;

    @PUT
    @Path("{participantId}/" + COMPENSATE)
    @Produces(MediaType.TEXT_PLAIN)
    @Compensate
    public Response compensate(@PathParam("participantId") String participantId,
            @HeaderParam(LRA_HTTP_CONTEXT_HEADER) String lraId,
            @HeaderParam(LRA_HTTP_PARENT_CONTEXT_HEADER) String parentId) {
        return toHttp(getParticipant(participantId).compensate(createURI(lraId), createURI(parentId)));
    }

    @PUT
    @Path("{participantId}/" + COMPLETE)
    @Produces(MediaType.TEXT_PLAIN)
    @Complete
    public Response complete(@PathParam("participantId") String participantId,
            @HeaderParam(LRA_HTTP_CONTEXT_HEADER) String lraId,
            @HeaderParam(LRA_HTTP_PARENT_CONTEXT_HEADER) String parentId) {
        return toHttp(getParticipant(participantId).complete(createURI(lraId), createURI(parentId)));
    }

    @GET
    @Path("{participantId}/" + STATUS)
    @Produces(MediaType.TEXT_PLAIN)
    @Status
    public Response status(@PathParam("participantId") String participantId,
            @HeaderParam(LRA_HTTP_CONTEXT_HEADER) String lraId,
            @HeaderParam(LRA_HTTP_PARENT_CONTEXT_HEADER) String parentId) {
        return toHttp(getParticipant(participantId).status(createURI(lraId), createURI(parentId)));
    }

    @DELETE
    @Path("{participantId}/" + FORGET)
    @Produces(MediaType.TEXT_PLAIN)
    @Forget
    public Response forget(@PathParam("participantId") String participantId,
            @HeaderParam(LRA_HTTP_CONTEXT_HEADER) String lraId,
            @HeaderParam(LRA_HTTP_PARENT_CONTEXT_HEADER) String parentId) {
        return toHttp(getParticipant(participantId).forget(createURI(lraId), createURI(parentId)));
    }

    @PUT
    @Path("{participantId}/" + AFTER)
    @AfterLRA
    public Response afterLRA(@PathParam("participantId") String participantId,
            @HeaderParam(LRA_HTTP_ENDED_CONTEXT_HEADER) URI lraId,
            LRAStatus lraStatus) {
        return toHttp(getParticipant(participantId).afterLRA(lraId, lraStatus));
    }

    private LRAParticipant getParticipant(String participantId) {
        LRAParticipant participant = lraParticipantRegistry.getParticipant(participantId);
        if (participant == null) {
            String errMsg = LRAProxyLogger.i18NLogger.error_missingParticipant(participantId);
            throw new WebApplicationException(errMsg, Response.status(Response.Status.NOT_FOUND)
                    .entity(errMsg)
                    .build());
        }
        return participant;
    }

    private URI createURI(String value) {
        return value != null ? URI.create(value) : null;
    }

    // map a participant notification result onto the HTTP response understood by the coordinator
    private static Response toHttp(CallbackResult result) {
        Response.ResponseBuilder builder = Response.status(httpStatus(result.getStatus()));
        if (result.getBody() != null) {
            builder.entity(result.getBody());
        }
        if (result.getUpdatedStatusCallback() != null) {
            builder.header(HttpHeaders.LOCATION, result.getUpdatedStatusCallback());
        }
        return builder.build();
    }

    private static Response.Status httpStatus(CallbackStatus status) {
        return switch (status) {
            case OK -> Response.Status.OK;
            case ACCEPTED -> Response.Status.ACCEPTED;
            case GONE -> Response.Status.GONE;
            case FAILED -> Response.Status.CONFLICT;
            case TIMEOUT, ERROR -> Response.Status.INTERNAL_SERVER_ERROR;
        };
    }
}
