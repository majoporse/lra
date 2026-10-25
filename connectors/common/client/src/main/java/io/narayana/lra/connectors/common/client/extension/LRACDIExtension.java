/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.extension;

import io.narayana.lra.LRAConstants;
import io.narayana.lra.connectors.common.client.extension.spi.ClassPathIndexer;
import io.narayana.lra.connectors.common.client.extension.spi.LegacyParticipantFactory;
import io.narayana.lra.connectors.common.client.internal.ParticipantProtocolRegistry;
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
 * Publishes the participant registry for whichever participant form the deployment uses.
 * <p>
 * The runtime switch {@code lra.server.filter.interceptor} selects the form: the legacy
 * ({@code @Path}/MicroProfile) participants when it is off, the transport-neutral
 * ({@code @LRA.Transaction}) participants when it is on. The registry bean is registered in both
 * modes, empty when the mode does not collect any participant, so the inbound adapters stay
 * resolvable.
 * <p>
 * This is the only CDI extension for participant discovery; it indexes the classpath once.
 */
public class LRACDIExtension implements Extension {

    public void observe(@Observes AfterBeanDiscovery event, BeanManager beanManager)
            throws IOException, ClassNotFoundException {
        Index index = new ClassPathIndexer().createIndex();
        Map<String, Participant> participants = new HashMap<>();

        if (LRAConstants.isInterceptorMode()) {
            new ContractParticipantDiscovery(index).collect(participants, beanManager);
            requireCallbackBaseUri(participants);
        } else {
            new LegacyParticipantDiscovery(index, legacyParticipantFactory()).collect(participants, beanManager);
        }

        registerRegistry(event, beanManager, LRAParticipantRegistry.class, new LRAParticipantRegistry(participants));
        // the protocol registry is needed by both participant worlds
        registerRegistry(event, beanManager, ParticipantProtocolRegistry.class, new ParticipantProtocolRegistry());
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

    private static <T> void registerRegistry(AfterBeanDiscovery event, BeanManager beanManager, Class<T> registryClass,
            T registry) {
        event.addBean()
                .read(beanManager.createAnnotatedType(registryClass))
                .beanClass(registryClass)
                .scope(jakarta.enterprise.context.ApplicationScoped.class)
                .createWith(context -> registry);
    }
}
