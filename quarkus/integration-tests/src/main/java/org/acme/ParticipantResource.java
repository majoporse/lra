/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package org.acme;

import io.narayana.lra.connectors.common.client.participant.LRAParticipantRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;

@Path("/participants")
@ApplicationScoped
public class ParticipantResource {

    @Inject
    LRAParticipantRegistry registry;

    @GET
    @Path("{id}")
    public boolean hasParticipant(@PathParam("id") String id) {
        return registry.getParticipant(id) != null;
    }
}
