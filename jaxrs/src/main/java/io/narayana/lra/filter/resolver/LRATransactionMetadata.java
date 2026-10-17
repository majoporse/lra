/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.filter.resolver;

import io.narayana.lra.api.LRA.Propagation;
import jakarta.ws.rs.core.Response;

/**
 * The transport-neutral view of the LRA attributes declared on a method, produced by an
 * {@link LRAAnnotationResolver}.
 */
public class LRATransactionMetadata {

    private final Propagation propagation;
    private final boolean end;
    private final Long timeLimitMillis;
    private final Response.Status[] cancelOn;
    private final Response.Status.Family[] cancelOnFamily;

    LRATransactionMetadata(Propagation propagation, boolean end, Long timeLimitMillis,
            Response.Status[] cancelOn, Response.Status.Family[] cancelOnFamily) {
        this.propagation = propagation;
        this.end = end;
        this.timeLimitMillis = timeLimitMillis;
        this.cancelOn = cancelOn;
        this.cancelOnFamily = cancelOnFamily;
    }

    public Propagation getPropagation() {
        return propagation;
    }

    public boolean isEnd() {
        return end;
    }

    public Long getTimeLimitMillis() {
        return timeLimitMillis;
    }

    public Response.Status[] getCancelOn() {
        return cancelOn;
    }

    public Response.Status.Family[] getCancelOnFamily() {
        return cancelOnFamily;
    }
}
