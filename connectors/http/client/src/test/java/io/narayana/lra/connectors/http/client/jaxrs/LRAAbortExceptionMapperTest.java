/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.jaxrs;

import io.narayana.lra.connectors.common.client.LRAAbortException;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LRAAbortExceptionMapperTest {

    @Test
    public void writesTheAbortMessageAsTheEntity() {
        LRAAbortException exception = new LRAAbortException(412, "MANDATORY but no tx");

        try (Response response = new LRAAbortExceptionMapper().toResponse(exception)) {
            Assertions.assertEquals(412, response.getStatus());
            Assertions.assertEquals("MANDATORY but no tx", response.getEntity());
        }
    }
}
