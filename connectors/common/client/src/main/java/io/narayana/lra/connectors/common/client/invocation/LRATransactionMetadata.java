/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.invocation;

import io.narayana.lra.api.Outcome;
import java.util.Set;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

/**
 * The transport-neutral view of the LRA attributes declared on a method, produced by an
 * {@link LRAAnnotationResolver}.
 * <p>
 * Cancel conditions are expressed as transport-neutral {@link Outcome families} plus the exact
 * numeric codes the transport may use (HTTP status codes today; another transport can leave the
 * set empty and rely on its own outcome).
 */
public class LRATransactionMetadata {

    private final LRA.Type propagation;
    private final boolean end;
    private final Long timeLimitMillis;
    private final Set<Integer> cancelOn;
    private final Set<Outcome> cancelOnFamily;

    public LRATransactionMetadata(LRA.Type propagation, boolean end, Long timeLimitMillis,
            Set<Integer> cancelOn, Set<Outcome> cancelOnFamily) {
        this.propagation = propagation;
        this.end = end;
        this.timeLimitMillis = timeLimitMillis;
        this.cancelOn = cancelOn;
        this.cancelOnFamily = cancelOnFamily;
    }

    public LRA.Type getPropagation() {
        return propagation;
    }

    public boolean isEnd() {
        return end;
    }

    public Long getTimeLimitMillis() {
        return timeLimitMillis;
    }

    public Set<Integer> getCancelOn() {
        return cancelOn;
    }

    public Set<Outcome> getCancelOnFamily() {
        return cancelOnFamily;
    }
}
