/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.inbound;

import static io.narayana.lra.LRAConstants.AFTER;
import static io.narayana.lra.LRAConstants.COMPENSATE;
import static io.narayana.lra.LRAConstants.COMPLETE;
import static io.narayana.lra.LRAConstants.FORGET;
import static io.narayana.lra.LRAConstants.STATUS;

import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.client.CallbackReceiver;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.ParticipantRequest;
import io.narayana.lra.connectors.http.contracts.HttpAfterLRACallback;
import io.narayana.lra.connectors.http.contracts.HttpCompensateCallback;
import io.narayana.lra.connectors.http.contracts.HttpCompleteCallback;
import io.narayana.lra.connectors.http.contracts.HttpForgetCallback;
import io.narayana.lra.connectors.http.contracts.HttpStatusCallback;
import io.narayana.lra.connectors.http.support.HttpContractPaths;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@ApplicationScoped
@Path(HttpContractPaths.RESOURCE_PATH)
public class LRACallbackResource {

    @Inject
    private CallbackReceiver callbackReceiver;

    @POST
    @Path("{participantId}/" + COMPENSATE)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HttpCompensateCallback.Reply compensate(@PathParam("participantId") String participantId,
            HttpCompensateCallback.Request request) {
        return HttpCompensateCallback.Reply.from(dispatch(CallbackType.COMPENSATE, request, participantId));
    }

    @POST
    @Path("{participantId}/" + COMPLETE)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HttpCompleteCallback.Reply complete(@PathParam("participantId") String participantId,
            HttpCompleteCallback.Request request) {
        return HttpCompleteCallback.Reply.from(dispatch(CallbackType.COMPLETE, request, participantId));
    }

    @POST
    @Path("{participantId}/" + STATUS)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HttpStatusCallback.Reply status(@PathParam("participantId") String participantId,
            HttpStatusCallback.Request request) {
        return HttpStatusCallback.Reply.from(dispatch(CallbackType.STATUS, request, participantId));
    }

    @POST
    @Path("{participantId}/" + FORGET)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HttpForgetCallback.Reply forget(@PathParam("participantId") String participantId,
            HttpForgetCallback.Request request) {
        return HttpForgetCallback.Reply.from(dispatch(CallbackType.FORGET, request, participantId));
    }

    @POST
    @Path("{participantId}/" + AFTER)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public HttpAfterLRACallback.Reply afterLRA(@PathParam("participantId") String participantId,
            HttpAfterLRACallback.Request request) {
        return HttpAfterLRACallback.Reply.from(dispatch(CallbackType.AFTER, request, participantId));
    }

    private ParticipantReply dispatch(CallbackType type, ParticipantRequest request, String participantId) {
        if (request != null) {
            request.participantId = participantId;
        }

        return callbackReceiver.receive(type, request);
    }
}
