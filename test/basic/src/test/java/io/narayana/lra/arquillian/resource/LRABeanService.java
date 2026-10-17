/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.arquillian.resource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * A thin HTTP facade used to drive the plain {@link LRABeanParticipant} bean from the test. The
 * LRA lifecycle is managed by the CDI interceptor on the bean, not on this resource, which itself
 * carries no LRA annotations.
 */
@ApplicationScoped
@Path(LRABeanService.RESOURCE_PATH)
public class LRABeanService {

    public static final String RESOURCE_PATH = "bean-lra";
    public static final String CLOSE_PATH = "/close";
    public static final String CANCEL_PATH = "/cancel";
    public static final String RESET_PATH = "/reset";
    public static final String STATE_PATH = "/state";

    @Inject
    LRABeanParticipant participant;

    @PUT
    @Path(CLOSE_PATH)
    public Response close() {
        participant.doWork();
        return Response.ok("closed").build();
    }

    @PUT
    @Path(CANCEL_PATH)
    public Response cancel() {
        try {
            participant.doFailingWork();
        } catch (IllegalStateException expected) {
            // the interceptor cancelled the LRA (and the coordinator compensated the participant)
            // before re-throwing the failure
        }

        return Response.ok("cancelled").build();
    }

    @PUT
    @Path(RESET_PATH)
    public Response reset() {
        participant.reset();
        return Response.ok("reset").build();
    }

    @GET
    @Path(STATE_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String state() {
        return String.format("compensate:%d complete:%d after:%d statuses:%s",
                participant.compensateCount(), participant.completeCount(),
                participant.afterCount(), participant.afterStatuses());
    }
}