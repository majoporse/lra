/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import io.narayana.lra.LRAConstants;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.CompletionStage;

/**
 * JAX-RS REST Client interface for LRA Coordinator operations.
 * This is the internal HTTP-specific client — not used directly by application code.
 * Use {@link CoordinatorClient} (the abstract interface) instead.
 *
 * <p>
 * Maps to the endpoints defined in {@code io.narayana.lra.coordinator.api.Coordinator}.
 * All methods return {@code CompletionStage} for asynchronous operations.
 * </p>
 */
public interface HttpCoordinatorClient {

    @GET
    @Path("/")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    CompletionStage<Response> getAllLRAs(
            @QueryParam(LRAConstants.STATUS_PARAM_NAME) @DefaultValue("") String status,
            @HeaderParam(HttpHeaders.ACCEPT) @DefaultValue(MediaType.TEXT_PLAIN) String accept,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version);

    @GET
    @Path("{LraId}/status")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    CompletionStage<Response> getLRAStatus(
            @PathParam("LraId") String lraId,
            @HeaderParam(HttpHeaders.ACCEPT) @DefaultValue(MediaType.TEXT_PLAIN) String accept,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version);

    @GET
    @Path("{LraId}")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    CompletionStage<Response> getLRAInfo(
            @PathParam("LraId") String lraId,
            @HeaderParam(HttpHeaders.ACCEPT) @DefaultValue(MediaType.TEXT_PLAIN) String accept,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version);

    @POST
    @Path("start")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    CompletionStage<Response> startLRA(
            @QueryParam(LRAConstants.CLIENT_ID_PARAM_NAME) @DefaultValue("") String clientId,
            @QueryParam(LRAConstants.TIMELIMIT_PARAM_NAME) @DefaultValue("0") Long timeLimit,
            @QueryParam(LRAConstants.PARENT_LRA_PARAM_NAME) @DefaultValue("") String parentLRA,
            @HeaderParam(HttpHeaders.ACCEPT) @DefaultValue(MediaType.TEXT_PLAIN) String accept,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version);

    @PUT
    @Path("{LraId}/renew")
    CompletionStage<Response> renewTimeLimit(
            @PathParam("LraId") String lraId,
            @QueryParam(LRAConstants.TIMELIMIT_PARAM_NAME) @DefaultValue("0") Long timeLimit,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version);

    @PUT
    @Path("{LraId}")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    CompletionStage<Response> joinLRA(
            @PathParam("LraId") String lraId,
            @QueryParam(LRAConstants.TIMELIMIT_PARAM_NAME) @DefaultValue("0") long timeLimit,
            @HeaderParam("Link") @DefaultValue("") String compensatorLink,
            @HeaderParam(HttpHeaders.ACCEPT) @DefaultValue(MediaType.TEXT_PLAIN) String accept,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version,
            @HeaderParam(LRAConstants.NARAYANA_LRA_PARTICIPANT_DATA_HEADER_NAME) @DefaultValue("") String participantData,
            String compensatorBody);

    @PUT
    @Path("{LraId}/remove")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    CompletionStage<Response> leaveLRA(
            @PathParam("LraId") String lraId,
            @HeaderParam(HttpHeaders.ACCEPT) @DefaultValue(MediaType.TEXT_PLAIN) String accept,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version,
            String participantCompensatorUrl);

    @PUT
    @Path("{LraId}/close")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    CompletionStage<Response> closeLRA(
            @PathParam("LraId") String lraId,
            @HeaderParam(HttpHeaders.ACCEPT) @DefaultValue(MediaType.TEXT_PLAIN) String accept,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version,
            @HeaderParam(LRAConstants.NARAYANA_LRA_PARTICIPANT_LINK_HEADER_NAME) @DefaultValue("") String compensator,
            @HeaderParam(LRAConstants.NARAYANA_LRA_PARTICIPANT_DATA_HEADER_NAME) @DefaultValue("") String userData);

    @PUT
    @Path("{LraId}/cancel")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    CompletionStage<Response> cancelLRA(
            @PathParam("LraId") String lraId,
            @HeaderParam(HttpHeaders.ACCEPT) @DefaultValue(MediaType.TEXT_PLAIN) String accept,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version,
            @HeaderParam(LRAConstants.NARAYANA_LRA_PARTICIPANT_LINK_HEADER_NAME) @DefaultValue("") String compensator,
            @HeaderParam(LRAConstants.NARAYANA_LRA_PARTICIPANT_DATA_HEADER_NAME) @DefaultValue("") String userData);

    @GET
    @Path("nested/{NestedLraId}/status")
    CompletionStage<Response> getNestedLRAStatus(@PathParam("NestedLraId") String nestedLraId);

    @PUT
    @Path("nested/{NestedLraId}/complete")
    CompletionStage<Response> completeNestedLRA(
            @PathParam("NestedLraId") String nestedLraId,
            @HeaderParam(HttpHeaders.ACCEPT) @DefaultValue(MediaType.TEXT_PLAIN) String accept,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version);

    @PUT
    @Path("nested/{NestedLraId}/compensate")
    CompletionStage<Response> compensateNestedLRA(
            @PathParam("NestedLraId") String nestedLraId,
            @HeaderParam(HttpHeaders.ACCEPT) @DefaultValue(MediaType.TEXT_PLAIN) String accept,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version);

    @DELETE
    @Path("nested/{NestedLraId}/forget")
    CompletionStage<Response> forgetNestedLRA(@PathParam("NestedLraId") String nestedLraId);
}
