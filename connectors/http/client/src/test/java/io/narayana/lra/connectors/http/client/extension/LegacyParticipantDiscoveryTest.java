/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client.extension;

import io.narayana.lra.connectors.common.client.extension.LegacyParticipantDiscovery;
import io.narayana.lra.connectors.common.client.participant.Participant;
import io.narayana.lra.connectors.http.client.extension.http.HttpLegacyParticipantFactory;
import io.narayana.lra.connectors.http.client.legacy.LegacyLRAParticipant;
import jakarta.enterprise.inject.spi.BeanManager;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.jboss.jandex.Index;
import org.jboss.jandex.Indexer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LegacyParticipantDiscoveryTest {

    @Test
    public void registersParticipantsThatOnlyDeclareJaxRsCallbacks() throws Exception {
        assertRegistered(JaxRsParticipant.class);
    }

    @Test
    public void registersParticipantsThatDeclareCallbacksWithoutEndpoints() throws Exception {
        assertRegistered(PlainParticipant.class);
    }

    @Test
    public void registersConcreteSubclassesOfAnAnnotatedClass() throws Exception {
        Map<String, Participant> participants = discover(AbstractParticipant.class, ConcreteParticipant.class);

        Assertions.assertTrue(participants.containsKey(ConcreteParticipant.class.getName()),
                ConcreteParticipant.class.getName() + " should have been discovered");
    }

    @Test
    public void registersParticipantsThatDeclareTheAnnotationsOnAnInterface() throws Exception {
        Map<String, Participant> participants = discover(ParticipantInterface.class, InterfaceParticipant.class);

        Assertions.assertTrue(participants.containsKey(InterfaceParticipant.class.getName()),
                InterfaceParticipant.class.getName() + " should have been discovered");
    }

    @Test
    public void skipsAbstractClasses() throws Exception {
        Map<String, Participant> participants = discover(AbstractParticipant.class, ConcreteParticipant.class);

        Assertions.assertFalse(participants.containsKey(AbstractParticipant.class.getName()));
    }

    @Test
    public void skipsClassesThatAreNotLraParticipants() throws Exception {
        Map<String, Participant> participants = discover(NotAParticipant.class);

        Assertions.assertTrue(participants.isEmpty());
    }

    private static void assertRegistered(Class<?> javaClass) throws Exception {
        Map<String, Participant> participants = discover(javaClass);

        Assertions.assertTrue(participants.containsKey(javaClass.getName()),
                javaClass.getName() + " should have been discovered");
        Assertions.assertInstanceOf(LegacyLRAParticipant.class, participants.get(javaClass.getName()));
    }

    private static Map<String, Participant> discover(Class<?>... classes) throws Exception {
        Map<String, Participant> participants = new HashMap<>();

        LegacyParticipantDiscovery.create(index(classes), new HttpLegacyParticipantFactory())
                .collect(participants, beanManager());

        return participants;
    }

    private static Index index(Class<?>... classes) throws IOException {
        Indexer indexer = new Indexer();

        for (Class<?> javaClass : classes) {
            try (InputStream stream = javaClass.getResourceAsStream(
                    "/" + javaClass.getName().replace('.', '/') + ".class")) {
                indexer.index(stream);
            }
        }

        return indexer.complete();
    }

    private static BeanManager beanManager() {
        return (BeanManager) Proxy.newProxyInstance(BeanManager.class.getClassLoader(),
                new Class<?>[] { BeanManager.class },
                (proxy, method, args) -> method.getName().equals("getBeans") ? Collections.emptySet() : null);
    }

    @jakarta.ws.rs.Path("/jaxrs")
    public static class JaxRsParticipant {

        @jakarta.ws.rs.GET
        @jakarta.ws.rs.Path("/work")
        @LRA(LRA.Type.REQUIRED)
        public String work() {
            return "ok";
        }

        @jakarta.ws.rs.PUT
        @jakarta.ws.rs.Path("/compensate")
        @Compensate
        public ParticipantStatus compensate(URI lraId) {
            return ParticipantStatus.Compensated;
        }
    }

    @jakarta.ws.rs.Path("/plain")
    public static class PlainParticipant {

        @jakarta.ws.rs.GET
        @jakarta.ws.rs.Path("/work")
        @LRA(LRA.Type.REQUIRED)
        public String work() {
            return "ok";
        }

        @Compensate
        public ParticipantStatus compensate(URI lraId) {
            return ParticipantStatus.Compensated;
        }
    }

    @jakarta.ws.rs.Path("/abstract")
    public abstract static class AbstractParticipant {

        @jakarta.ws.rs.GET
        @jakarta.ws.rs.Path("/work")
        @LRA(LRA.Type.REQUIRED)
        public abstract String work();

        @jakarta.ws.rs.PUT
        @jakarta.ws.rs.Path("/compensate")
        @Compensate
        public ParticipantStatus compensate(URI lraId) {
            return ParticipantStatus.Compensated;
        }
    }

    public static class ConcreteParticipant extends AbstractParticipant {

        @Override
        public String work() {
            return "ok";
        }
    }

    public interface ParticipantInterface {

        @jakarta.ws.rs.GET
        @jakarta.ws.rs.Path("/work")
        @LRA(LRA.Type.REQUIRED)
        String work();
    }

    @jakarta.ws.rs.Path("/interface")
    public static class InterfaceParticipant implements ParticipantInterface {

        @Override
        public String work() {
            return "ok";
        }

        @jakarta.ws.rs.PUT
        @jakarta.ws.rs.Path("/compensate")
        @Compensate
        public ParticipantStatus compensate(URI lraId) {
            return ParticipantStatus.Compensated;
        }
    }

    @jakarta.ws.rs.Path("/not-a-participant")
    public static class NotAParticipant {

        @jakarta.ws.rs.GET
        @jakarta.ws.rs.Path("/work")
        public String work() {
            return "ok";
        }
    }
}
