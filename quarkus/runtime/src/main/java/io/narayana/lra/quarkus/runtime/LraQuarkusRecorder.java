/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.quarkus.runtime;

import io.narayana.lra.connectors.common.client.extension.ParticipantForm;
import io.narayana.lra.connectors.common.client.extension.spi.LegacyParticipantFactory;
import io.narayana.lra.connectors.common.client.participant.LRAParticipant;
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

    public RuntimeValue<LRAParticipantRegistry> participantRegistry(
            ParticipantForm form,
            List<String> participantClasses) {
        Map<String, Participant> participants = new HashMap<>();

        if (form == ParticipantForm.LEGACY) {
            LegacyParticipantFactory factory = legacyParticipantFactory();

            if (!participantClasses.isEmpty() && factory == null) {
                throw new IllegalStateException("No LegacyParticipantFactory is registered for the legacy participants");
            }

            for (String className : participantClasses) {
                Participant participant = factory.create(load(className));

                if (participant != null) {
                    participants.put(participant.participantId(), participant);
                }
            }
        } else {
            for (String className : participantClasses) {
                LRAParticipant participant = new LRAParticipant(load(className));
                participants.put(participant.participantId(), participant);
            }
        }

        return new RuntimeValue<>(new LRAParticipantRegistry(participants));
    }

    private static LegacyParticipantFactory legacyParticipantFactory() {
        for (LegacyParticipantFactory factory : ServiceLoader.load(LegacyParticipantFactory.class)) {
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
