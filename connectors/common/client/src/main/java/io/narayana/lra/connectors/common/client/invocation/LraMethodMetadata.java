/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.invocation;

import io.narayana.lra.api.Outcome;
import java.util.Set;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

/**
 * The transport-neutral view of the method-level LRA declarations: the transaction attributes
 * ({@code @LRA}) plus the standalone {@code @Leave} and {@code @PropagateToken} markers.
 * <p>
 * It is resolved once per intercepted method and stored on the invocation. Cancel conditions are
 * expressed as transport-neutral {@link Outcome families} plus the exact numeric codes the
 * transport may use (HTTP status codes today; another transport can leave the set empty and rely on
 * its own outcome).
 */
public final class LraMethodMetadata {

    private final LRA.Type propagation;
    private final boolean end;
    private final Long timeLimitMillis;
    private final Set<Integer> cancelOn;
    private final Set<Outcome> cancelOnFamily;
    private final boolean leave;
    private final boolean participantCallback;
    private final boolean propagateToken;

    public LraMethodMetadata(LRA.Type propagation, boolean end, Long timeLimitMillis,
            Set<Integer> cancelOn, Set<Outcome> cancelOnFamily, boolean leave,
            boolean participantCallback, boolean propagateToken) {
        this.propagation = propagation;
        this.end = end;
        this.timeLimitMillis = timeLimitMillis;
        this.cancelOn = cancelOn;
        this.cancelOnFamily = cancelOnFamily;
        this.leave = leave;
        this.participantCallback = participantCallback;
        this.propagateToken = propagateToken;
    }

    /**
     * @return the declared propagation type, or {@code null} when the method carries no {@code @LRA}
     */
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

    public boolean isLeave() {
        return leave;
    }

    public boolean isParticipantCallback() {
        return participantCallback;
    }

    public boolean isPropagateToken() {
        return propagateToken;
    }

    /**
     * A participant callback and a leave-only method run with the incoming context but are not
     * business work that should be enlisted.
     */
    public boolean isEndAnnotation() {
        return participantCallback || leave;
    }

    /**
     * @return whether the interceptor should manage this method at all
     */
    public boolean isManaged() {
        return propagation != null || leave || propagateToken;
    }
}
