/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.extension;

import io.narayana.lra.connectors.common.client.extension.spi.LegacyParticipantFactory;
import io.narayana.lra.connectors.common.client.participant.Participant;
import jakarta.enterprise.inject.spi.BeanManager;
import java.util.Map;
import org.jboss.jandex.IndexView;

/**
 * Builds the legacy (MicroProfile annotation) participant models found by
 * {@link ParticipantScanner}.
 */
public final class LegacyParticipantDiscovery {

    private final ParticipantDiscoverySupport support;
    private final LegacyParticipantFactory factory;
    private final ParticipantScanner scanner = new ParticipantScanner();

    private LegacyParticipantDiscovery(IndexView index, LegacyParticipantFactory factory) {
        this.support = new ParticipantDiscoverySupport(index);
        this.factory = factory;
    }

    public static LegacyParticipantDiscovery create(IndexView index, LegacyParticipantFactory factory) {
        return new LegacyParticipantDiscovery(index, factory);
    }

    public void collect(Map<String, Participant> participants, BeanManager beanManager)
            throws ClassNotFoundException {
        if (factory == null) {
            return;
        }

        for (ParticipantDescriptor descriptor : scanner.scan(support.index(), ParticipantForm.LEGACY)) {
            Class<?> javaClass = support.loadClass(descriptor.className());
            Participant participant = factory.create(javaClass);

            if (participant == null || !participant.hasCallbacks()
                    || participants.putIfAbsent(participant.participantId(), participant) != null) {
                continue;
            }

            support.bindBeanInstance(javaClass, beanManager, participant::setInstance);
        }
    }
}
