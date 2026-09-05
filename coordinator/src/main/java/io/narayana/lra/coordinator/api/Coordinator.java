/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.api;

import static io.narayana.lra.LRAConstants.API_VERSION_1_0;
import static io.narayana.lra.LRAConstants.API_VERSION_1_1;
import static io.narayana.lra.LRAConstants.API_VERSION_1_2;
import static io.narayana.lra.LRAConstants.API_VERSION_1_3;
import static io.narayana.lra.LRAConstants.COORDINATOR_PATH_NAME;
import static io.narayana.lra.LRAConstants.CURRENT_API_VERSION_STRING;
import static io.narayana.lra.LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME;
import static io.narayana.lra.LRAConstants.PARTICIPANT_TIMEOUT;
import static io.narayana.lra.LRAConstants.RECOVERY_COORDINATOR_PATH_NAME;
import static io.narayana.lra.LRAConstants.STATUS_PARAM_NAME;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static jakarta.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import static jakarta.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.eclipse.microprofile.lra.annotation.ws.rs.LRA.LRA_HTTP_RECOVERY_HEADER;

import io.narayana.lra.Current;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.LRAData;
import io.narayana.lra.contracts.http.CancelLRAHttp;
import io.narayana.lra.contracts.http.CloseLRAHttp;
import io.narayana.lra.contracts.http.GetAllLRAHttp;
import io.narayana.lra.contracts.http.GetLRAInfoLRAHttp;
import io.narayana.lra.contracts.http.JoinLRAHttp;
import io.narayana.lra.contracts.http.LeaveLRAHttp;
import io.narayana.lra.contracts.http.ParticipantLinks;
import io.narayana.lra.contracts.http.RenewTimeLimitLRAHttp;
import io.narayana.lra.contracts.http.StartLRAHttp;
import io.narayana.lra.contracts.http.StatusLRAHttp;
import io.narayana.lra.coordinator.domain.model.LongRunningAction;
import io.narayana.lra.coordinator.domain.service.HttpLRAService;
import io.narayana.lra.coordinator.domain.service.LRAService;
import io.narayana.lra.coordinator.internal.LRARecoveryModule;
import io.narayana.lra.coordinator.security.JwtTokenContext;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.ws.rs.ApplicationPath;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Application;
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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.faulttolerance.Bulkhead;
import org.eclipse.microprofile.lra.annotation.LRAStatus;
import org.eclipse.microprofile.openapi.annotations.Components;
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.headers.Header;
import org.eclipse.microprofile.openapi.annotations.info.Contact;
import org.eclipse.microprofile.openapi.annotations.info.Info;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@ApplicationScoped
@ApplicationPath("/")
@Path(COORDINATOR_PATH_NAME)
@OpenAPIDefinition(info = @Info(title = "LRA Coordinator", version = LRAConstants.API_VERSION_2_0, contact = @Contact(name = "Narayana", url = "https://narayana.io")), tags = @Tag(name = "LRA Coordinator"), components = @Components(schemas = {
        @Schema(name = "LRAApiVersionSchema", description = "Format is `major.minor`, both components are required, they are to be numbers", type = SchemaType.STRING, pattern = "^\\d+\\.\\d+$", example = "1.0")
}, parameters = {
        @Parameter(name = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME, in = ParameterIn.HEADER, description = "Narayana LRA API version", schema = @Schema(ref = "LRAApiVersionSchema"))
}, headers = {
        @Header(name = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME, description = "Narayana LRA API version", schema = @Schema(ref = "LRAApiVersionSchema"))
}))
@Tag(name = "LRA Coordinator", description = "Operations to work with active LRAs (to start, to get a status, to finish, etc.)")
public class Coordinator extends Application {
    @Context
    private UriInfo context;

    private static final boolean allowParticipantData = initAllowParticipantData();

    private final LRAService lraService;
    private final HttpLRAService httpLraService;
    private final RecoveryCoordinator recoveryCoordinator;
    private final NestedCoordinator nestedCoordinator;

    public Coordinator() {
        lraService = LRARecoveryModule.getService();
        httpLraService = LRARecoveryModule.getHttpService();
        recoveryCoordinator = new RecoveryCoordinator();
        nestedCoordinator = new NestedCoordinator();
    }

    @Path(RECOVERY_COORDINATOR_PATH_NAME)
    public RecoveryCoordinator getRecoveryCoordinator() {
        return recoveryCoordinator;
    }

    @Path(LRAConstants.NESTED_COORDINATOR_PATH_NAME)
    public NestedCoordinator getNestedCoordinator() {
        return nestedCoordinator;
    }

    private static boolean initAllowParticipantData() {
        try {
            // We cannot inject it using @ConfigProperty(name = LRAConstants.ALLOW_PARTICIPANT_DATA,defaultValue = "true")
            // because CDI injection isn't guaranteed in JAX-RS Application classes
            return ConfigProvider.getConfig().getValue(LRAConstants.ALLOW_PARTICIPANT_DATA, Boolean.class);
        } catch (Exception e) {
            return true; // the property is unset or there is no config provider so use the default value
        }
    }

    private boolean isAllowParticipantData(String version) {
        // only protocol version API_VERSION_1_0 doesn't support participant data
        // and using a null version header is interpreted as meaning the caller doesn't care
        return (version == null) || (allowParticipantData && !version.equals(API_VERSION_1_0));
    }

    @GET
    @Path("/")
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(summary = "Returns all LRAs", description = "Gets both active and recovering LRAs")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "The LRAData json array which is known to coordinator", content = @Content(schema = @Schema(type = SchemaType.ARRAY, implementation = LRAData.class)), headers = {
                    @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "400", description = "Provided Status is not recognized as a valid LRA status value", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                    @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "417", description = "The requested version provided in HTTP Header is not supported by this end point", content = @Content(schema = @Schema(implementation = String.class))),
    })
    public GetAllLRAHttp.Reply getAllLRAs(
            @Parameter(name = STATUS_PARAM_NAME, description = "Filter the returned LRAs to only those in the give state (see CompensatorStatus)") @QueryParam(STATUS_PARAM_NAME) LRAStatus status,
            @Parameter(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @DefaultValue(CURRENT_API_VERSION_STRING) String version) {
        List<LRAData> lras = lraService.getAll(status);
        return new GetAllLRAHttp.Reply(lras);
    }

    @GET
    @Path("{LraId}/status")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    @Operation(summary = "Obtain the status of an LRA as a string")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "The LRA exists and the status is reported in the content body."
                    + " The status may be any LRAStatus value: Active, Closing, Cancelling,"
                    + " Closed, Cancelled, FailedToClose, or FailedToCancel.", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                            @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "404", description = "The coordinator has no knowledge of this LRA", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "417", description = "The requested version provided in HTTP Header is not supported by this end point", content = @Content(schema = @Schema(implementation = String.class))),
    })
    public StatusLRAHttp.Reply getLRAStatus(
            @Parameter(name = "LraId", description = "The unique identifier of the LRA." +
                    "Expecting to be a valid URL where the participant can be contacted at. If not in URL format it will be considered "
                    +
                    "to be an id which will be declared to exist at URL where coordinator is deployed at.", required = true) @PathParam("LraId") String lraId,
            @Parameter(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @DefaultValue(CURRENT_API_VERSION_STRING) String version) {
        LongRunningAction transaction = httpLraService.getTransaction(toURI(lraId));
        LRAStatus status = transaction.getLRAStatus();

        if (status == null) {
            status = LRAStatus.Active;
        }

        return new StatusLRAHttp.Reply(status);
    }

    @GET
    @Path("{LraId}")
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(summary = "Obtain the information about an LRA as a JSON structure")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "The LRA exists and the information is packed as JSON in the content body.", content = @Content(schema = @Schema(implementation = LRAData.class)), headers = {
                    @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "404", description = "The coordinator has no knowledge of this LRA", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "417", description = "The requested version provided in HTTP Header is not supported by this end point", content = @Content(schema = @Schema(implementation = String.class))),
    })
    public GetLRAInfoLRAHttp.Reply getLRAInfo(
            @Parameter(name = "LraId", description = "The unique identifier of the LRA", required = true) @PathParam("LraId") String lraId,
            @Parameter(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @DefaultValue(CURRENT_API_VERSION_STRING) String version) {
        URI lraIdURI = toURI(lraId);
        LRAData lraData = httpLraService.getLRA(lraIdURI);
        return new GetLRAInfoLRAHttp.Reply(lraData);
    }

    /**
     * Performing a POST on {@value LRAConstants#COORDINATOR_PATH_NAME}/start?ClientID={ClientID}
     * will start a new lra with a default timeout and return an LRA URL
     * of the form {coordinator url}/{@value LRAConstants#COORDINATOR_PATH_NAME}/{LraId}.
     * Adding a query parameter, {@value LRAConstants#TIMELIMIT_PARAM_NAME}={timeout}, will start a new lra with the specified
     * timeout.
     * If the lra is terminated because of a timeout, the lra URL is deleted and all further invocations on the URL will return
     * 404.
     * The invoker can assume this was equivalent to a compensation operation.
     */
    @POST
    @Path("start")
    @Produces({ MediaType.APPLICATION_JSON })
    @Bulkhead
    @Operation(summary = "Start a new LRA", description = "The LRA model uses a presumed nothing protocol: the coordinator must communicate "
            + "with participants in order to inform them of the LRA activity. Every time a "
            + "Compensator is enrolled with an LRA, the coordinator must make information about "
            + "it durable so that the Compensator can be contacted when the LRA terminates, "
            + "even in the event of subsequent failures. Participants, clients and coordinators "
            + "cannot make any presumption about the state of the global transaction without "
            + "consulting the coordinator and all participants, respectively.")
    @APIResponses({
            @APIResponse(responseCode = "201", description = "The request was successful and the response body contains the id of the new LRA", content = @Content(schema = @Schema(description = "An URI of the new LRA", implementation = String.class)), headers = {
                    @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "404", description = "Parent LRA id cannot be joint to the started LRA", content = @Content(schema = @Schema(description = "Message containing problematic LRA id", implementation = String.class))),
            @APIResponse(responseCode = "417", description = "The requested version provided in HTTP Header is not supported by this end point", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "500", description = "A new LRA could not be started. Coordinator internal error.", content = @Content(schema = @Schema(implementation = String.class)))
    })
    public StartLRAHttp.Reply startLRA(
            @RequestBody StartLRAHttp.Request body,
            @Parameter(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @DefaultValue(CURRENT_API_VERSION_STRING) String version) {

        URI parentId = body.parentLRA;
        var timelimit = body.timeout == null ? 0 : body.timeout;
        var clientId = body.clientId == null ? "" : body.clientId;
        String coordinatorUrl = String.format("%s%s", context.getBaseUri(), COORDINATOR_PATH_NAME);
        LongRunningAction lra = httpLraService.startLRA(coordinatorUrl, parentId, clientId, timelimit);
        String hierarchy = lra.getParentHierarchy();
        URI lraId = hierarchy != null
                ? URI.create(coordinatorUrl + "/" + lra.getId().toString() + "?" + LRAConstants.PARENT_LRA_PARAM_NAME + "="
                        + hierarchy)
                : URI.create(coordinatorUrl + "/" + lra.getId().toString());

        if (parentId != null) {
            // the startLRA call will have imported the parent LRA
            String compensatorUrl = String.format("%s/%s/%s", coordinatorUrl, LRAConstants.NESTED_COORDINATOR_PATH_NAME,
                    LRAConstants.getLRAUid(lraId));

            if (!httpLraService.hasTransaction(parentId)) {

                try (Client client = JwtTokenContext.newClient()) {
                    try (Response response = client.target(parentId)
                            .request()
                            .header(NARAYANA_LRA_API_VERSION_HEADER_NAME, CURRENT_API_VERSION_STRING)
                            .async()
                            .put(Entity.text(compensatorUrl))
                            .get(PARTICIPANT_TIMEOUT, TimeUnit.SECONDS)) {

                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            String errMessage = String.format("The coordinator at %s returned an unexpected response: %d"
                                    + "when the LRA '%s' tried to join the parent LRA '%s'", parentId, response.getStatus(),
                                    lraId, parentId);
                            throw new WebApplicationException(errMessage, response.getStatus());
                        }
                    }
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    String errMsg = String.format(
                            "Cannot contact the LRA Coordinator at '%s' for LRA '%s' joining parent LRA '%s'",
                            parentId, lraId, parentId);
                    LRALogger.logger.info(errMsg);
                    // don't include the root exception (it should already be in the server side logs):
                    throw new WebApplicationException(errMsg, Response.status(INTERNAL_SERVER_ERROR)
                            .header(NARAYANA_LRA_API_VERSION_HEADER_NAME, version)
                            .entity(errMsg)
                            .build());
                }
            }
        }

        Current.push(lraId);

        return new StartLRAHttp.Reply(lraId, Current.getContexts());
    }

    @PUT
    @Path("{LraId}/renew")
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(summary = "Update the TimeLimit for an existing LRA", description = "LRAs can be automatically cancelled if they aren't closed or cancelled before the TimeLimit "
            + "specified at creation time is reached. The time limit can be updated to postpone (extend) the timeout, but cannot be shortened. "
            + "If the new timeout is earlier than the current one, the request will be ignored and return 200 OK without making any changes.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "If the LRA time limit has been updated", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                    @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "404", description = "The coordinator has no knowledge of this LRA or " +
                    "the LRA is not longer active (ie the complete or compensate messages have been sent", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                            @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "417", description = "The requested version provided in HTTP Header is not supported by this end point", content = @Content(schema = @Schema(implementation = String.class))),
    })
    public RenewTimeLimitLRAHttp.Reply renewTimeLimit(
            @Parameter(name = "LraId", description = "The unique identifier of the LRA", required = true) @PathParam("LraId") String lraId,
            @Parameter(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @DefaultValue(CURRENT_API_VERSION_STRING) String version,
            @RequestBody RenewTimeLimitLRAHttp.Request body) {
        try {
            var status = httpLraService.renewTimeLimit(toURI(lraId), body.timeLimit);
            if (status < 200 || status >= 300) {
                throw new WebApplicationException(status);
            }
            return new RenewTimeLimitLRAHttp.Reply(lraId);
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Performing a PUT on {@value LRAConstants#COORDINATOR_PATH_NAME}/{LraId}/close will trigger the successful completion
     * of the LRA and all participants will be dropped by the LRA Coordinator.
     * The complete message will be sent to the participants.
     * Upon termination, the URL is implicitly deleted. If it no longer exists, then 404 will be returned.
     * The invoker cannot know for sure whether the lra completed or compensated without enlisting a participant.
     */
    @PUT
    @Path("{LraId}/close")
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(summary = "Attempt to close an LRA", description = "Trigger the successful completion of the LRA. All"
            + " participants will be dropped by the coordinator."
            + " The complete message will be sent to the participants."
            + " Upon termination, the URL is implicitly deleted."
            + " The invoker cannot know for sure whether the lra completed"
            + " or compensated without enlisting a participant.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "The LRA closed successfully and all participants have completed."
                    + " The response body contains the terminal LRA status.", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                            @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "202", description = "The close request has been accepted but one or more participants have not yet responded."
                    + " The response body contains the current LRA status (Closing)."
                    + " A Location header points to the status endpoint for polling."
                    + " Clients should poll the status endpoint to track progress."
                    + " Requires Narayana-LRA-API-version 2.0 or later;"
                    + " older versions receive 200 for all states.", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                            @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "404", description = "No LRA exists with the given identifier", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                    @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "412", description = "The LRA is no longer in an active state and cannot be closed", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "417", description = "The requested API version is not supported by this endpoint", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "503", description = "The coordinator could not process the request due to a transient failure"
                    + " (storage unavailable or lock contention with another close/cancel in progress)."
                    + " The client should retry the request.", content = @Content(schema = @Schema(implementation = String.class))),
    })
    public CloseLRAHttp.Reply closeLRA(
            @Parameter(name = "LraId", description = "The unique identifier of the LRA", required = true) @PathParam("LraId") String lraId,
            @Parameter(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @DefaultValue(CURRENT_API_VERSION_STRING) String version,
            CloseLRAHttp.Request body) {

        var participantId = body.participantId;
        var userData = body.userData;
        try {
            URI lraURI = toURI(lraId);
            LRAData lraData = httpLraService.endLRA(lraURI, false, false, participantId, userData);

            var lraStatus = lraData.getStatus();
            if (!isTerminal(lraStatus) && !(lraStatus == LRAStatus.Closing || lraStatus == LRAStatus.Cancelling)) {
                throw new WebApplicationException(SERVICE_UNAVAILABLE);
            }

            return new CloseLRAHttp.Reply(lraData.getStatus());
        } catch (WebApplicationException e) {
            LRALogger.logger.debug(e.getMessage());
            // catch it otherwise the caller just sees a generic message corresponding to e.getResponse().getStatus()
            // eg for a 503 it would be "Service Unavailable"
            // and if we throw new WebApplicationException(e.getMessage(), e);
            // then the caller sees the generic 500 Internal Server Error code rather than the specific 503 code
            throw new WebApplicationException(Response.status(e.getResponse().getStatus())
                    .entity(e.getMessage())
                    .header(NARAYANA_LRA_API_VERSION_HEADER_NAME, version)
                    .build());
        }
    }

    @PUT
    @Path("{LraId}/cancel")
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(summary = "Attempt to cancel an LRA", description = " Trigger the compensation of the LRA. All"
            + " participants will be triggered by the coordinator (ie the compensate message will be sent to each participants)."
            + " Upon termination, the URL is implicitly deleted."
            + " The invoker cannot know for sure whether the lra completed or compensated without enlisting a participant.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "The LRA cancelled successfully and all participants have compensated."
                    + " The response body contains the terminal LRA status.", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                            @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "202", description = "The cancel request has been accepted but one or more participants have not yet responded."
                    + " The response body contains the current LRA status (Cancelling)."
                    + " A Location header points to the status endpoint for polling."
                    + " Clients should poll the status endpoint to track progress."
                    + " Requires Narayana-LRA-API-version 2.0 or later;"
                    + " older versions receive 200 for all states.", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                            @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "404", description = "No LRA exists with the given identifier", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                    @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "412", description = "The LRA is no longer in an active state and cannot be cancelled", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "417", description = "The requested API version is not supported by this endpoint", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "503", description = "The coordinator could not process the request due to a transient failure"
                    + " (storage unavailable or lock contention with another close/cancel in progress)."
                    + " The client should retry the request.", content = @Content(schema = @Schema(implementation = String.class))),
    })
    public CancelLRAHttp.Reply cancelLRA(
            @Parameter(name = "LraId", description = "The unique identifier of the LRA", required = true) @PathParam("LraId") String lraId,
            @Parameter(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @DefaultValue(CURRENT_API_VERSION_STRING) String version,
            @RequestBody CancelLRAHttp.Request body) {

        var compensator = body.compensator == null ? "" : body.compensator;
        var userData = body.userData == null ? "" : body.userData;

        try {
            URI lraURI = toURI(lraId);
            LRAData lraData = httpLraService.endLRA(lraURI, true, false, compensator, userData);
            var lraStatus = lraData.getStatus();

            if (!isTerminal(lraStatus) && !(lraStatus == LRAStatus.Closing || lraStatus == LRAStatus.Cancelling)) {
                throw new WebApplicationException(SERVICE_UNAVAILABLE);
            }

            return new CancelLRAHttp.Reply(
                    lraData.getStatus().name());
        } catch (WebApplicationException e) {
            LRALogger.logger.debug(e.getMessage());
            throw new WebApplicationException(Response.status(e.getResponse().getStatus())
                    .entity(e.getMessage())
                    .header(NARAYANA_LRA_API_VERSION_HEADER_NAME, version)
                    .build());
        }
    }

    @PUT
    @Path("{LraId}")
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(summary = "A Compensator can join with the LRA at any time prior to the completion of an activity")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "The participant was successfully registered with the LRA", content = @Content(schema = @Schema(description = "A URI representing the recovery id of this join request", implementation = String.class)), headers = {
                    @Header(name = LRA_HTTP_RECOVERY_HEADER, description = "It contains a unique resource reference for that participant:\n"
                            + " - HTTP GET on the reference returns the original participant URL;\n" // Note that isn't a test for this
                            + " - HTTP PUT on the reference will overwrite the old participant URL with the new one supplied.", schema = @Schema(implementation = String.class)),
                    @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "400", description = "Link does not contain all required fields for joining the LRA. " +
                    "Probably no compensator or after 'rel' is available.", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "404", description = "The coordinator has no knowledge of this LRA", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "412", description = "The LRA is not longer active (ie the complete or compensate message has been sent), or wrong format of compensator data", content = @Content(schema = @Schema(implementation = String.class)), headers = {
                    @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "417", description = "The requested version provided in HTTP Header is not supported by this end point", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "500", description = "Format of the compensator data (e.g. Link format) could not be processed", content = @Content(schema = @Schema(implementation = String.class))),
    })
    public JoinLRAHttp.Reply joinLRAViaBody(
            @Parameter(name = "LraId", description = "The unique identifier of the LRA", required = true) @PathParam("LraId") String lraId,
            @Parameter(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @DefaultValue(CURRENT_API_VERSION_STRING) String version,
            @RequestBody JoinLRAHttp.Request body) {

        var timeLimit = body.timeLimit == null ? 0 : body.timeLimit;
        var userData = body.userData == null ? "" : body.userData;
        var compensatorLink = body.compensatorLink == null ? "" : body.compensatorLink;

        var partId = body.partId;

        // test to see if the join request contains any participant specific data
        if (userData != null && !userData.isEmpty() && !isAllowParticipantData(version)) {
            String errMsg = LRALogger.i18nLogger.error_participant_data_disallowed(lraId);
            LRALogger.logger.error(errMsg);

            throw new WebApplicationException(errMsg, Response.status(PRECONDITION_FAILED)
                    .entity(errMsg)
                    .header(NARAYANA_LRA_API_VERSION_HEADER_NAME, version)
                    .build());
        }

        StringBuilder sb = new StringBuilder();

        if (userData != null) {
            sb.append(userData);
        }

        return joinLRA(toURI(lraId), timeLimit, body.links, sb, version, partId);
    }

    private JoinLRAHttp.Reply joinLRA(URI lraId, long timeLimit, ParticipantLinks links,
            StringBuilder userData, String version, String participantId) {
        final String recoveryUrlBase = String.format("%s%s/%s",
                context.getBaseUri().toASCIIString(), COORDINATOR_PATH_NAME, RECOVERY_COORDINATOR_PATH_NAME);

        if (userData == null) {
            userData = new StringBuilder();
        }

        StringBuilder recoveryUrl = new StringBuilder();
        int status;

        try {
            status = httpLraService.joinLRA(recoveryUrl, lraId, timeLimit, links, recoveryUrlBase, userData,
                    version, participantId);
            if (status < 200 || status >= 300) {
                String errMessage = String.format(
                        "Failed to join LRA '%s'. Coordinator returned status: %d", lraId, status);
                throw new WebApplicationException(errMessage, Response.status(status).build());
            }
        } catch (ServiceUnavailableException e) {
            throw new WebApplicationException(e.getMessage(),
                    Response.status(SERVICE_UNAVAILABLE).entity(e.getMessage()).build());
        }

        return new JoinLRAHttp.Reply(
                recoveryUrl.toString(),
                userData.toString());
    }

    /**
     * A participant can resign from an LRA at any time prior to the completion of an activity by performing a
     * PUT on {@value LRAConstants#COORDINATOR_PATH_NAME}/{LraId}/remove with the URL of the participant.
     */
    @PUT
    @Path("{LraId}/remove")
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(summary = "A Compensator can resign from the LRA at any time prior to the completion of an activity")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "If the participant was successfully removed from the LRA", headers = {
                    @Header(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) }),
            @APIResponse(responseCode = "400", description = "The coordinator has no knowledge of this participant compensator URL", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "404", description = "The coordinator has no knowledge of this LRA", content = @Content(schema = @Schema(implementation = String.class))),
            @APIResponse(responseCode = "412", description = "The LRA is not longer active (ie in the complete or compensate messages have been sent"),
            @APIResponse(responseCode = "417", description = "The requested version provided in HTTP Header is not supported by this end point", content = @Content(schema = @Schema(implementation = String.class))),
    })
    public LeaveLRAHttp.Reply leaveLRA(
            @Parameter(name = "LraId", description = "The unique identifier of the LRA", required = true) @PathParam("LraId") String lraId,
            @Parameter(ref = LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @HeaderParam(LRAConstants.NARAYANA_LRA_API_VERSION_HEADER_NAME) @DefaultValue(CURRENT_API_VERSION_STRING) String version,
            LeaveLRAHttp.Request body) {
        int status = httpLraService.leave(toURI(lraId), body.participantId);
        if (status < 200 || status >= 300) {
            throw new WebApplicationException(status);
        }

        return new LeaveLRAHttp.Reply("ok");
    }

    private Response buildResponse(LRAStatus lraStatus, String apiVersion, String mediaType, URI lraId) {
        // API version 2.0+ distinguishes three cases:
        //   200 OK       — terminal state (Closed, Cancelled, FailedToClose, FailedToCancel)
        //   202 Accepted — transitional state (Closing, Cancelling): the coordinator tried
        //                   to end the LRA but participants haven't all responded yet
        //   503 Service Unavailable — Active: the coordinator could not process the request
        //                   (e.g. lock contention), client should retry
        // Older versions always return 200 for backward compatibility.
        int httpStatus;
        if (isTerminal(lraStatus)) {
            httpStatus = Response.Status.OK.getStatusCode();
        } else if (!supportsAcceptedStatus(apiVersion)) {
            httpStatus = Response.Status.OK.getStatusCode(); // legacy behavior
        } else if (lraStatus == LRAStatus.Closing || lraStatus == LRAStatus.Cancelling) {
            httpStatus = Response.Status.ACCEPTED.getStatusCode();
        } else {
            // Active after a close/cancel call means the coordinator could not process
            // the request (e.g. lock contention). Signal the client to retry.
            httpStatus = Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
        }

        String statusName = lraStatus.name();

        Response.ResponseBuilder builder = Response.status(httpStatus)
                .header(NARAYANA_LRA_API_VERSION_HEADER_NAME, apiVersion);

        // For 202 responses, include a Location header pointing to the status endpoint
        // so clients know where to poll for the outcome.
        if (httpStatus == Response.Status.ACCEPTED.getStatusCode() && lraId != null) {
            URI statusUri = URI.create(String.format("%s%s/%s/status",
                    context.getBaseUri(), COORDINATOR_PATH_NAME, LRAConstants.getLRAUid(lraId)));
            builder.location(statusUri);
        }

        if (mediaType.equals(MediaType.APPLICATION_JSON)) {
            JsonObject model = Json.createObjectBuilder()
                    .add("status", statusName)
                    .build();
            return builder.entity(model.toString()).build();
        } else { // produce MediaType.TEXT_PLAIN
            return builder.entity(statusName).build();
        }
    }

    private static boolean isTerminal(LRAStatus status) {
        return status == LRAStatus.Closed || status == LRAStatus.Cancelled
                || status == LRAStatus.FailedToClose || status == LRAStatus.FailedToCancel;
    }

    /**
     * Returns true if the client API version supports 202 Accepted responses
     * for non-terminal/transitional LRA states. Versions prior to 2.0 always
     * received 200 regardless of LRA state.
     *
     * Defaults to false (legacy behavior) when the version header is absent,
     * null, or unrecognised so that existing clients are not surprised by 202.
     */
    private static boolean supportsAcceptedStatus(String version) {
        return version != null
                && !version.equals(API_VERSION_1_0)
                && !version.equals(API_VERSION_1_1)
                && !version.equals(API_VERSION_1_2)
                && !version.equals(API_VERSION_1_3);
    }

    private URI toURI(String lraId) {
        URL url;
        // needed to decode string passed from clients
        String decodedURL = URLDecoder.decode(lraId, StandardCharsets.UTF_8);

        try {
            // see if it already in the correct format
            url = new URL(decodedURL);
            url.toURI();
        } catch (Exception e) {
            try {
                url = new URL(String.format("%s%s/%s", context.getBaseUri(), COORDINATOR_PATH_NAME, lraId));
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
