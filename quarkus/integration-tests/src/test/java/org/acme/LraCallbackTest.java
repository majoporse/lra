/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package org.acme;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class LraCallbackTest {

    @Test
    public void participantIsDiscovered() {
        given()
                .get("/participants/org.acme.WorkflowService")
                .then()
                .statusCode(200)
                .body(is("true"));
    }

    @Test
    public void completeCallbackIsDispatched() {
        given()
                .contentType(MediaType.APPLICATION_JSON)
                .body("{\"lraId\":\"lra://0b6b0b4e-9c0b-4a93-8f37-6f5f64f1f6f5\"}")
                .post("/lra-callback-proxy/org.acme.WorkflowService/complete")
                .then()
                .statusCode(200)
                .body("status", equalTo("Completed"));
    }
}
