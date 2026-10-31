/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.extension;

import io.narayana.lra.connectors.common.client.participant.LRAParticipant;
import io.narayana.lra.connectors.common.client.participant.Participant;
import jakarta.enterprise.inject.spi.BeanManager;
import java.util.Map;
import org.jboss.jandex.IndexView;

/**
 * Builds the transport-neutral participant models found by {@link ParticipantScanner}.
 */
final class ContractParticipantDiscovery {

    private final ParticipantDiscoverySupport support;
    private final ParticipantScanner scanner = new ParticipantScanner();

    ContractParticipantDiscovery(IndexView index) {
        this.support = new ParticipantDiscoverySupport(index);
    }

    void collect(Map<String, Participant> participants, BeanManager beanManager)
            throws ClassNotFoundException {
        for (ParticipantDescriptor descriptor : scanner.scan(support.index(), ParticipantForm.CONTRACT)) {
            Class<?> javaClass = support.loadClass(descriptor.className());
            LRAParticipant participant = new LRAParticipant(javaClass);

            if (participants.putIfAbsent(participant.participantId(), participant) == null) {
                support.bindBeanInstance(javaClass, beanManager, participant::setInstance);
            }
        }
    }
}
