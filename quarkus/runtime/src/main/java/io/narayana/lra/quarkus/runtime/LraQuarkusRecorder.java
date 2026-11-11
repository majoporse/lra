/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.quarkus.runtime;

import io.narayana.lra.connectors.common.client.discovery.MicroProfileParticipantFactory;
import io.narayana.lra.connectors.common.client.discovery.ParticipantDiscovery;
import io.narayana.lra.connectors.common.client.participant.LRAParticipantRegistry;
import io.narayana.lra.connectors.common.client.participant.Participant;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Creates the participant registry from the classes discovered by the Quarkus build step.
 */
@Recorder
public class LraQuarkusRecorder {

    public RuntimeValue<LRAParticipantRegistry> participantRegistry(List<String> participantClasses) {
        Map<String, Participant> participants = new HashMap<>();
        MicroProfileParticipantFactory factory = microProfileParticipantFactory();

        for (String className : participantClasses) {
            Participant participant = ParticipantDiscovery.select(load(className), factory);

            if (participant != null && participant.hasCallbacks()) {
                ParticipantDiscovery.requireTermination(participant);
                participants.put(participant.participantId(), participant);
            }
        }

        return new RuntimeValue<>(new LRAParticipantRegistry(participants));
    }

    private static MicroProfileParticipantFactory microProfileParticipantFactory() {
        for (MicroProfileParticipantFactory factory : ServiceLoader.load(MicroProfileParticipantFactory.class)) {
            return factory;
        }

        return null;
    }

    private static Class<?> load(String className) {
        try {
            return Class.forName(className, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Cannot load LRA participant " + className, e);
        }
    }
}
