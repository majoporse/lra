/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client;

import io.narayana.lra.LRAConstants;
import io.narayana.lra.contracts.http.CancelLRAHttp;
import io.narayana.lra.contracts.http.CloseLRAHttp;
import io.narayana.lra.contracts.http.GetAllLRAHttp;
import io.narayana.lra.contracts.http.GetLRAInfoLRAHttp;
import io.narayana.lra.contracts.http.JoinLRAHttp;
import io.narayana.lra.contracts.http.LeaveLRAHttp;
import io.narayana.lra.contracts.http.NestedCompleteLRAHttp;
import io.narayana.lra.contracts.http.NestedStatusLRAHttp;
import io.narayana.lra.contracts.http.RenewTimeLimitLRAHttp;
import io.narayana.lra.contracts.http.StartLRAHttp;
import io.narayana.lra.contracts.http.StatusLRAHttp;
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
import org.eclipse.microprofile.lra.annotation.LRAStatus;

/**
 * MicroProfile REST Client interface for LRA Coordinator operations with asynchronous support.
 * This interface maps to the endpoints defined in io.narayana.lra.coordinator.api.Coordinator
 *
 * All methods return CompletionStage for asynchronous operations.
 *
 * This client is designed to be used programmatically via RestClientBuilder:
 *
 * <pre>
 * CoordinatorClient client = RestClientBuilder.newBuilder()
 *         .baseUri(coordinatorUri)
 *         .build(CoordinatorClient.class);
 * </pre>
 */
public interface CoordinatorClient {

    /**
     * Get all LRAs known to the coordinator
     *
     * @param version API version header
     * @return Response containing list of LRAs
     */
    @GET
    @Path("/")
    @Produces({ MediaType.APPLICATION_JSON })
    CompletionStage<GetAllLRAHttp.Reply> getAllLRAs(
            @QueryParam(LRAConstants.STATUS_PARAM_NAME) LRAStatus status,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version);

    /**
     * Get the status of a specific LRA
     *
     * @param lraId LRA identifier
     * @param version API version header
     * @return Response containing LRA status
     */
    @GET
    @Path("{LraId}/status")
    @Produces({ MediaType.APPLICATION_JSON })
    CompletionStage<StatusLRAHttp.Reply> getLRAStatus(
            @PathParam("LraId") String lraId,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version);

    /**
     * Get detailed information about a specific LRA
     *
     * @param lraId LRA identifier
     * @param version API version header
     * @return Response containing LRA data
     */
    @GET
    @Path("{LraId}")
    @Produces({ MediaType.APPLICATION_JSON })
    CompletionStage<GetLRAInfoLRAHttp.Reply> getLRAInfo(
            @PathParam("LraId") String lraId,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version);

    /**
     * Start a new LRA
     *
     * @param version API version header
     * @return Response with new LRA ID in Location header
     */
    @POST
    @Path("start")
    @Produces({ MediaType.APPLICATION_JSON })
    CompletionStage<StartLRAHttp.Reply> startLRA(
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version,
            StartLRAHttp.Request body);

    /**
     * Renew the time limit for an existing LRA
     *
     * @param lraId LRA identifier
     * @param version API version header
     * @return Response indicating success or failure
     */
    @PUT
    @Path("{LraId}/renew")
    CompletionStage<RenewTimeLimitLRAHttp.Reply> renewTimeLimit(
            @PathParam("LraId") String lraId,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version,
            RenewTimeLimitLRAHttp.Request body);

    /**
     * Join a participant to an LRA
     *
     * @param lraId LRA identifier
     * @param version API version header
     * @return Response with recovery URL in header
     */
    @PUT
    @Path("{LraId}")
    @Produces({ MediaType.APPLICATION_JSON })
    CompletionStage<JoinLRAHttp.Reply> joinLRA(
            @PathParam("LraId") String lraId,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version,
            JoinLRAHttp.Request body);

    /**
     * Remove a participant from an LRA
     *
     * @param lraId LRA identifier
     * @param version API version header
     * @return Response indicating success or failure
     */
    @PUT
    @Path("{LraId}/remove")
    @Produces({ MediaType.APPLICATION_JSON })
    CompletionStage<LeaveLRAHttp.Reply> leaveLRA(
            @PathParam("LraId") String lraId,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version,
            LeaveLRAHttp.Request body);

    /**
     * Close (complete) an LRA
     *
     * @param lraId LRA identifier
     * @param version API version header
     * @return Response containing LRA status
     */
    @PUT
    @Path("{LraId}/close")
    @Produces({ MediaType.APPLICATION_JSON })
    CompletionStage<CloseLRAHttp.Reply> closeLRA(
            @PathParam("LraId") String lraId,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version,
            CloseLRAHttp.Request body);

    /**
     * Cancel (compensate) an LRA
     *
     * @param lraId LRA identifier
     * @param version API version header
     * @return Response containing LRA status
     */
    @PUT
    @Path("{LraId}/cancel")
    @Produces({ MediaType.APPLICATION_JSON })
    CompletionStage<CancelLRAHttp.Reply> cancelLRA(
            @PathParam("LraId") String lraId,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version,
            CancelLRAHttp.Request body);

    /**
     * Get status of a nested LRA
     *
     * @param nestedLraId Nested LRA identifier
     * @return Response with nested LRA status
     */
    @GET
    @Path("nested/{NestedLraId}/status")
    @Produces({ MediaType.APPLICATION_JSON })
    CompletionStage<NestedStatusLRAHttp.Reply> getNestedLRAStatus(@PathParam("NestedLraId") String nestedLraId);

    /**
     * Complete a nested LRA
     *
     * @param nestedLraId Nested LRA identifier
     * @param version API version header
     * @return Response with participant status
     */
    @PUT
    @Path("nested/{NestedLraId}/complete")
    CompletionStage<NestedCompleteLRAHttp.Reply> completeNestedLRA(
            @PathParam("NestedLraId") String nestedLraId,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version);

    /**
     * Compensate a nested LRA
     *
     * @param nestedLraId Nested LRA identifier
     * @param accept Media type for response
     * @param version API version header
     * @return Response with participant status
     */
    @PUT
    @Path("nested/{NestedLraId}/compensate")
    CompletionStage<Response> compensateNestedLRA(
            @PathParam("NestedLraId") String nestedLraId,
            @HeaderParam(HttpHeaders.ACCEPT) @DefaultValue(MediaType.TEXT_PLAIN) String accept,
            @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) String version);

    /**
     * Forget a nested LRA
     *
     * @param nestedLraId Nested LRA identifier
     * @return Response indicating success
     */
    @DELETE
    @Path("nested/{NestedLraId}/forget")
    CompletionStage<Response> forgetNestedLRA(@PathParam("NestedLraId") String nestedLraId);
}
