/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.outbound;

import jakarta.annotation.Priority;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.ext.Provider;
import org.jboss.resteasy.plugins.providers.jackson.ResteasyJackson2Provider;

/**
 * Jackson before JSON-B for the LRA client. The join body carries polymorphic callback addresses
 * whose Jackson type id JSON-B would drop.
 */
@Provider
@Priority(4000)
@Consumes({ MediaType.APPLICATION_JSON, "application/*+json", "text/json" })
@Produces({ MediaType.APPLICATION_JSON, "application/*+json", "text/json" })
public class LraJacksonProvider extends ResteasyJackson2Provider {
}
