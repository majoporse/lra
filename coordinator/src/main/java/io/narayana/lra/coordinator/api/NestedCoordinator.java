/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.api;

import static io.narayana.lra.LRAConstants.COORDINATOR_PATH_NAME;
import static io.narayana.lra.LRAConstants.CURRENT_API_VERSION_STRING;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

import io.narayana.lra.LRAConstants;
import io.narayana.lra.LRAData;
import io.narayana.lra.contracts.http.NestedCompensateLRAHttp;
import io.narayana.lra.contracts.http.NestedCompleteLRAHttp;
import io.narayana.lra.contracts.http.NestedForgetLRAHttp;
import io.narayana.lra.contracts.http.NestedStatusLRAHttp;
import io.narayana.lra.coordinator.domain.service.HttpLRAService;
import io.narayana.lra.coordinator.internal.LRARecoveryModule;
import io.narayana.lra.logging.LRALogger;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.headers.Header;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Tag(name = "Nested LRA Participant", description = "Implements the MicroProfile LRA participant contract (@Complete, @Compensate, @Status, @Forget)"
        + " for nested LRAs. These endpoints are called by the parent LRA coordinator.")
public class NestedCoordinator {
    private final HttpLRAService httpLraService;

    public NestedCoordinator() {
        httpLraService = LRARecoveryModule.getHttpService();
    }

    @GET
    @Path("{NestedLraId}/status")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    @Operation(summary = "Get the status of a nested LRA participant", description = "Implements the @Status participant contract as defined by the MicroProfile LRA specification."
            + " Returns the current ParticipantStatus of the nested LRA.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "The current ParticipantStatus is available."
                    + " The caller must inspect the body to determine whether the participant"
                    + " has reached a terminal state.", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "410", description = "The participant is no longer aware of this LRA.", content = @Content(schema = @Schema(implementation = String.class))),
    })
    public NestedStatusLRAHttp.Reply getNestedLRAStatus(
            @PathParam("NestedLraId") String nestedLraId,
            @Context UriInfo uriInfo) {
        try {
            LRAStatus status = httpLraService.getTransaction(toURI(nestedLraId, uriInfo)).getLRAStatus();

            if (status == null) {
                throw new WebApplicationException(
                        LRALogger.i18nLogger.error_cannotGetStatusOfNestedLraURI(nestedLraId, null),
                        Response.status(INTERNAL_SERVER_ERROR).build());
            }

            return new NestedStatusLRAHttp.Reply(mapToParticipantStatus(status));
        } catch (NotFoundException e) {
            throw new WebApplicationException(Response.Status.GONE);
        }
    }

    @PUT
    @Path("{NestedLraId}/complete")
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(summary = "Complete a nested LRA", description = "Implements the @Complete participant contract"
            + " for a nested LRA as defined by the MicroProfile LRA specification.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "The completion was successful.", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                    @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "202", description = "The completion is still in progress."
                    + " The caller should poll the @Status endpoint to determine the outcome.", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                            @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "409", description = "The completion failed."
                    + " The caller should use @Forget to release the participant.", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "410", description = "The participant is no longer aware of this LRA.", content = @Content(schema = @Schema(implementation = String.class))),
    })
    public NestedCompleteLRAHttp.Reply completeNestedLRA(
            @Parameter(name = "NestedLraId", description = "The unique identifier of the nested LRA", required = true) @PathParam("NestedLraId") String nestedLraId,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @DefaultValue(CURRENT_API_VERSION_STRING) String version,
            @Context UriInfo uriInfo) {

        try {
            LRAData lraData = httpLraService.endLRA(toURI(nestedLraId, uriInfo), false, false, null, null);
            ParticipantStatus pStatus = mapToParticipantStatus(lraData.getStatus());

            return new NestedCompleteLRAHttp.Reply(pStatus);

        } catch (NotFoundException e) {
            throw new WebApplicationException(Response.Status.GONE);
        }
    }

    @PUT
    @Path("{NestedLraId}/compensate")
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(summary = "Compensate a nested LRA", description = "Implements the @Compensate participant contract"
            + " for a nested LRA as defined by the MicroProfile LRA specification."
            + " Per the spec, a nested LRA that has already closed can still be asked"
            + " to compensate if the parent LRA cancels (Closed -> Cancelling LRA transition).")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "The compensation was successful.", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                    @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "202", description = "The compensation is still in progress."
                    + " The caller should poll the @Status endpoint to determine the outcome.", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                            @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "409", description = "The compensation failed."
                    + " The caller should use @Forget to release the participant.", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "410", description = "The participant is no longer aware of this LRA.", content = @Content(schema = @Schema(implementation = String.class))),
    })
    public NestedCompensateLRAHttp.Reply compensateNestedLRA(
            @Parameter(name = "NestedLraId", description = "The unique identifier of the nested LRA", required = true) @PathParam("NestedLraId") String nestedLraId,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @DefaultValue(CURRENT_API_VERSION_STRING) String version,
            @Context UriInfo uriInfo) {

        try {
            LRAData lraData = httpLraService.endLRA(toURI(nestedLraId, uriInfo), true, true, null, null);
            ParticipantStatus pStatus = mapToParticipantStatus(lraData.getStatus());
            return new NestedCompensateLRAHttp.Reply(pStatus);
        } catch (NotFoundException e) {
            throw new WebApplicationException(Response.Status.GONE);
        }
    }

    @DELETE
    @Path("{NestedLraId}/forget")
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(summary = "Forget a nested LRA participant", description = "Implements the @Forget participant contract"
            + " as defined by the MicroProfile LRA specification."
            + " Signals that the participant can clean up any resources associated with the LRA."
            + " Called after a participant has reported a final status (FailedToComplete or FailedToCompensate).")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "The participant has been forgotten successfully."),
            @APIResponse(responseCode = "410", description = "The participant is no longer aware of this LRA."),
    })
    public NestedForgetLRAHttp.Reply forgetNestedLRA(
            @PathParam("NestedLraId") String nestedLraId,
            @Context UriInfo uriInfo) {
        try {
            httpLraService.getTransaction(toURI(nestedLraId, uriInfo));
        } catch (NotFoundException e) {
            throw new WebApplicationException(Response.Status.GONE);
        }

        httpLraService.remove(toURI(nestedLraId, uriInfo));
        return new NestedForgetLRAHttp.Reply("ok");
    }

    private ParticipantStatus mapToParticipantStatus(LRAStatus lraStatus) {
        switch (lraStatus) {
            case Active:
                return ParticipantStatus.Active;
            case Closed:
                return ParticipantStatus.Completed;
            case Cancelled:
                return ParticipantStatus.Compensated;
            case Closing:
                return ParticipantStatus.Completing;
            case Cancelling:
                return ParticipantStatus.Compensating;
            case FailedToClose:
                return ParticipantStatus.FailedToComplete;
            case FailedToCancel:
                return ParticipantStatus.FailedToCompensate;
            default:
                String errMsg = LRALogger.i18nLogger.warn_invalid_lraStatus(String.valueOf(lraStatus));
                LRALogger.logger.debug(errMsg);
                throw new WebApplicationException(errMsg, Response.status(INTERNAL_SERVER_ERROR)
                        .entity(errMsg)
                        .build());
        }
    }

    private URI toURI(String lraId, UriInfo uriInfo) {
        URL url;
        String decodedURL = URLDecoder.decode(lraId, StandardCharsets.UTF_8);

        try {
            url = new URL(decodedURL);
            url.toURI();
        } catch (Exception e) {
            try {
                url = new URL(String.format("%s%s/%s", uriInfo.getBaseUri(), COORDINATOR_PATH_NAME, lraId));
            } catch (MalformedURLException e1) {
                String errMsg = LRALogger.i18nLogger.error_invalidStringFormatOfUrl(lraId, e1);
                LRALogger.logger.error(errMsg);
                throw new WebApplicationException(errMsg, Response.status(BAD_REQUEST)
                        .entity(errMsg)
                        .build());
            }
        }

        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            String errMsg = LRALogger.i18nLogger.error_invalidStringFormatOfUrl(lraId, e);
            LRALogger.logger.warn(errMsg);
            throw new WebApplicationException(errMsg, Response.status(BAD_REQUEST)
                    .entity(errMsg)
                    .build());
        }
    }
}
