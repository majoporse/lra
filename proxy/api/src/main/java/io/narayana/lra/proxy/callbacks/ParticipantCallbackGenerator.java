/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.proxy.callbacks;

import io.narayana.lra.callbacks.ParticipantCallbacks;
import io.narayana.lra.client.internal.proxy.nonjaxrs.LRAParticipant;

public interface ParticipantCallbackGenerator {

    /**
     * Builds the termination callbacks ({@code @Complete}, {@code @Compensate}, {@code @Status},
     * {@code @Forget} and {@code @AfterLRA}) for a non-JAX-RS participant based on the
     * configured transport, keeping any callbacks the participant already has.
     *
     * @param participant the participant to generate callbacks for
     * @param existing the callbacks already derived for the participant (e.g. from annotations)
     * @return the participant's callbacks with the missing slots filled by this generator
     */
    ParticipantCallbacks getCallbacks(LRAParticipant participant, ParticipantCallbacks existing);
}
