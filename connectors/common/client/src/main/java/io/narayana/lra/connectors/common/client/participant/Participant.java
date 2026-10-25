/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.participant;

import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.ParticipantCallbackAddressResolver;
import io.narayana.lra.connectors.common.contracts.ParticipantReply;
import io.narayana.lra.connectors.common.contracts.ParticipantRequest;
import java.net.URI;

/**
 * The participant-side model of one bean: the callbacks it declares, the addresses it registers and
 * how its callback methods are invoked.
 * <p>
 * There are two forms. {@link LRAParticipant} is the default: transport-neutral callback methods
 * that take and return the common callback contract. {@code
 * io.narayana.lra.connectors.http.client.legacy.LegacyLRAParticipant} is the MicroProfile form: JAX-RS resource
 * methods with {@code URI} arguments and {@code ParticipantStatus}/{@code Response} results.
 * <p>
 * Both are invoked through the same {@code CallbackReceiver} and looked up in the same
 * {@link LRAParticipantRegistry}; the form only selects how a callback method is validated and
 * called.
 */
public interface Participant {

    /**
     * @return the id the coordinator and the inbound adapters address this participant by
     */
    String participantId();

    /**
     * @return whether the bean declares at least one LRA callback
     */
    boolean hasCallbacks();

    /**
     * Bind the bean instance the callback methods are invoked on.
     */
    void setInstance(Object instance);

    /**
     * Build the addresses the coordinator should use for this participant.
     *
     * @param resolver the connector's address resolver (nullable when no container is available)
     * @param uriPrefix the base URI of the invocation, used by participant forms that register
     *        JAX-RS endpoints (nullable)
     * @param baseUri the configured callback base URI, used by forms that register generated
     *        endpoints (nullable)
     * @return the callback address book, never {@code null}
     */
    ParticipantCallbacks terminationCallbacks(ParticipantCallbackAddressResolver resolver, String uriPrefix,
            URI baseUri);

    /**
     * Invoke one callback.
     *
     * @return the reply, or {@code null} when the participant declares no method of that kind
     */
    ParticipantReply invoke(CallbackType type, ParticipantRequest request);
}
