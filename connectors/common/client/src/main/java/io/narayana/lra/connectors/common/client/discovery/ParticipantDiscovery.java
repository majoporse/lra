/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.discovery;

import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.client.participant.LRAParticipant;
import io.narayana.lra.connectors.common.client.participant.Participant;
import jakarta.enterprise.inject.spi.BeanManager;
import java.util.Map;
import java.util.ServiceLoader;
import org.jboss.jandex.IndexView;

/**
 * Builds the participant model for every class found by {@link ParticipantScanner}.
 * <p>
 * Each class uses one callback shape: the transport-neutral contract model when it declares any
 * contract-shaped callback, otherwise the MicroProfile model supplied by the protocol connector.
 * A class that mixes the two shapes is rejected instead of being wrapped.
 */
public final class ParticipantDiscovery {

    private final ParticipantDiscoverySupport support;
    private final MicroProfileParticipantFactory factory;
    private final ParticipantScanner scanner = new ParticipantScanner();

    public ParticipantDiscovery(IndexView index, MicroProfileParticipantFactory factory) {
        this.support = new ParticipantDiscoverySupport(index);
        this.factory = factory;
    }

    /**
     * @return the first MicroProfile participant factory on the classpath, or {@code null} when the
     *         deployment has no protocol connector that contributes one
     */
    public static MicroProfileParticipantFactory serviceLoadedFactory() {
        for (MicroProfileParticipantFactory factory : ServiceLoader.load(MicroProfileParticipantFactory.class)) {
            return factory;
        }

        return null;
    }

    /**
     * Choose the single callback shape a class uses.
     */
    public static Participant select(Class<?> javaClass, MicroProfileParticipantFactory factory) {
        Participant contract = new LRAParticipant(javaClass);

        if (contract.hasCallbacks()) {
            Participant legacy = factory == null ? null : factory.create(javaClass);

            if (legacy != null && legacy.hasCallbacks()) {
                throw new IllegalStateException(String.format(
                        "%s declares both the MicroProfile and the callback-contract shape; use one shape per participant",
                        javaClass.getName()));
            }

            return contract;
        }

        return factory == null ? null : factory.create(javaClass);
    }

    /**
     * A participant must declare at least one termination callback.
     */
    public static void requireTermination(Participant participant) {
        if (participant.hasCallbacks()
                && !participant.hasCallback(CallbackType.COMPENSATE)
                && !participant.hasCallback(CallbackType.COMPLETE)
                && !participant.hasCallback(CallbackType.AFTER)) {
            throw new IllegalStateException(String.format(
                    "%s declares no @Compensate, @Complete or @AfterLRA callback",
                    participant.participantId()));
        }
    }

    public void collect(Map<String, Participant> participants, BeanManager beanManager)
            throws ClassNotFoundException {
        for (ParticipantDescriptor descriptor : scanner.scan(support.index())) {
            Class<?> javaClass = support.loadClass(descriptor.className());
            Participant participant = select(javaClass, factory);

            if (participant == null || !participant.hasCallbacks()) {
                continue;
            }

            requireTermination(participant);

            if (participants.putIfAbsent(participant.participantId(), participant) != null) {
                continue;
            }

            support.bindBeanInstance(javaClass, beanManager, participant::setInstance);
        }
    }
}
