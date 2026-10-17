/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.arquillian;

import io.narayana.lra.arquillian.resource.LRABeanParticipant;
import io.narayana.lra.arquillian.resource.LRABeanService;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the transport-neutral participant callbacks ({@code @LRA.Compensate},
 * {@code @LRA.Complete}, {@code @LRA.AfterLRA}) are invoked on a plain (non-JAX-RS) CDI bean when
 * its LRA is cancelled or closed. The application runs in the CDI interceptor mode
 * ({@code lra.server.filter.interceptor=true}).
 */
public class LRABeanCallbackIT extends TestBase {

    @ArquillianResource
    public URL baseURL;

    @BeforeEach
    public void resetParticipant() {
        try (Response response = TestBase.client.target(UriBuilder.fromUri(baseURL.toExternalForm())
                .path(LRABeanService.RESOURCE_PATH).path(LRABeanService.RESET_PATH).build())
                .request().put(null)) {
            Assertions.assertEquals(200, response.getStatus());
        }
    }

    @Deployment
    public static WebArchive deploy() {
        return Deployer.deployBeanParticipantApp(LRABeanCallbackIT.class.getSimpleName(),
                // the participant WildFly runs with a port offset of 100 (the coordinator is on 8080)
                "http://localhost:8180/" + LRABeanCallbackIT.class.getSimpleName() + "/",
                LRABeanParticipant.class, LRABeanService.class);
    }

    @Test
    public void testCloseRunsCompleteAndAfterLRA() {
        try (Client client = ClientBuilder.newClient()) {
            Response response = client.target(UriBuilder.fromUri(baseURL.toExternalForm())
                    .path(LRABeanService.RESOURCE_PATH).path(LRABeanService.CLOSE_PATH).build())
                    .request().put(null);
            Assertions.assertEquals(200, response.getStatus());
            response.close();

            String state = awaitState(client, "complete:1", "after:1");
            Assertions.assertTrue(state.contains("statuses:[Closed]"),
                    "expected the after-LRA callback to report Closed but was: " + state);
        }
    }

    @Test
    public void testCancelRunsCompensate() {
        try (Client client = ClientBuilder.newClient()) {
            Response response = client.target(UriBuilder.fromUri(baseURL.toExternalForm())
                    .path(LRABeanService.RESOURCE_PATH).path(LRABeanService.CANCEL_PATH).build())
                    .request().put(null);
            Assertions.assertEquals(200, response.getStatus());
            response.close();

            awaitState(client, "compensate:1");
        }
    }

    // the callback invocations are performed by the coordinator, so poll the participant state
    // until all expected counters have been observed
    private String awaitState(Client client, String... expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        String lastState = null;

        while (System.nanoTime() < deadline) {
            Response response = getState(client);
            lastState = response.readEntity(String.class);
            response.close();

            if (Arrays.stream(expected).allMatch(lastState::contains)) {
                return lastState;
            }

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        Assertions.fail("Callbacks were not invoked as expected " + Arrays.toString(expected)
                + "; participant state: " + lastState);
        return lastState; // unreachable
    }

    private Response getState(Client client) {
        return client.target(UriBuilder.fromUri(baseURL.toExternalForm())
                .path(LRABeanService.RESOURCE_PATH).path(LRABeanService.STATE_PATH).build())
                .request().get();
    }
}