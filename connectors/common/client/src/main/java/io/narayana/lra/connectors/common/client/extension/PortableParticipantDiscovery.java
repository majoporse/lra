/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.extension;

import io.narayana.lra.LRAConstants;
import io.narayana.lra.connectors.common.client.extension.spi.ClassPathIndexer;
import io.narayana.lra.connectors.common.client.extension.spi.LegacyParticipantFactory;
import io.narayana.lra.connectors.common.client.participant.LRAParticipantRegistry;
import io.narayana.lra.connectors.common.client.participant.Participant;
import io.narayana.lra.connectors.common.config.CallbackConfig;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.AfterBeanDiscovery;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.DeploymentException;
import jakarta.enterprise.inject.spi.Extension;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.jboss.jandex.Index;

/**
 * Publishes the participant registry for portable CDI containers.
 * <p>
 * The runtime switch {@code lra.server.filter.interceptor} selects the form: the legacy
 * ({@code @Path}/MicroProfile) participants when it is off, the transport-neutral
 * ({@code @LRA.Transaction}) participants when it is on. The registry bean is registered in both
 * modes, empty when the mode does not collect any participant, so the inbound adapters stay
 * resolvable.
 * <p>
 * Quarkus does not run portable CDI extensions; {@code ParticipantDiscoveryProcessor} is the
 * build-time counterpart. Both use {@link ParticipantScanner}.
 */
public class PortableParticipantDiscovery implements Extension {

    public void observe(@Observes AfterBeanDiscovery event, BeanManager beanManager)
            throws IOException, ClassNotFoundException {
        Index index = new ClassPathIndexer().createIndex();
        Map<String, Participant> participants = new HashMap<>();

        if (LRAConstants.isInterceptorMode()) {
            new ContractParticipantDiscovery(index).collect(participants, beanManager);
            requireCallbackBaseUri(participants);
        } else {
            LegacyParticipantDiscovery.create(index, legacyParticipantFactory()).collect(participants, beanManager);
        }

        event.addBean()
                .read(beanManager.createAnnotatedType(LRAParticipantRegistry.class))
                .beanClass(LRAParticipantRegistry.class)
                .scope(jakarta.enterprise.context.ApplicationScoped.class)
                .createWith(context -> new LRAParticipantRegistry(participants));
    }

    private static LegacyParticipantFactory legacyParticipantFactory() {
        for (LegacyParticipantFactory factory : ServiceLoader.load(LegacyParticipantFactory.class)) {
            return factory;
        }

        return null;
    }

    private static void requireCallbackBaseUri(Map<String, Participant> participants) {
        if (participants.isEmpty() || isKafkaProtocol()) {
            return;
        }

        if (LRAConstants.getBaseUri().isEmpty()) {
            throw new DeploymentException(String.format(
                    "The transport-neutral LRA participants %s are enlisted with callback URIs built on the"
                            + " '%s' configuration property, which is not set. Set it to the base URI the LRA"
                            + " coordinator can use to reach this application, or remove the participant callbacks.",
                    participants.keySet().stream().sorted().collect(Collectors.joining(", ")),
                    LRAConstants.NARAYANA_LRA_BASE_URI_PROPERTY_NAME));
        }
    }

    private static boolean isKafkaProtocol() {
        try {
            return CallbackConfig.isKafka();
        } catch (RuntimeException e) {
            throw new DeploymentException(
                    "Cannot resolve the LRA callback protocol; configure '" + CallbackConfig.PROTOCOL_PROPERTY + "'"
                            + " and '" + CallbackConfig.SERVICE_TOPIC_PROPERTY + "' consistently",
                    e);
        }
    }
}
