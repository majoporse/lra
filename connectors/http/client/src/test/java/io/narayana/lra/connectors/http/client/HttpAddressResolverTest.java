/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.client;

import io.narayana.lra.LRAConstants;
import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.common.client.participant.LRAParticipant;
import io.narayana.lra.connectors.common.contracts.CompensateCallback;
import io.narayana.lra.connectors.common.contracts.CompleteCallback;
import io.narayana.lra.connectors.http.address.HttpContractAddress;
import io.narayana.lra.connectors.http.address.LegacyHttpAddress;
import io.narayana.lra.connectors.http.client.participant.HttpAddressResolver;
import io.narayana.lra.connectors.http.client.participant.MicroProfileLRAParticipant;
import java.net.URI;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HttpAddressResolverTest {

    private static final String BASE_URI = "http://service:8080";

    @BeforeEach
    public void configureBaseUri() {
        System.setProperty(LRAConstants.NARAYANA_LRA_BASE_URI_PROPERTY_NAME, BASE_URI);
    }

    @AfterEach
    public void clearBaseUri() {
        System.clearProperty(LRAConstants.NARAYANA_LRA_BASE_URI_PROPERTY_NAME);
    }

    @Test
    public void writesTheCallbackProxyAddresses() {
        ParticipantCallbacks callbacks = resolve(ProxyParticipant.class);

        HttpContractAddress compensate = (HttpContractAddress) callbacks.callback(CallbackType.COMPENSATE);

        Assertions.assertEquals(BASE_URI + "/lra-callback-proxy/" + ProxyParticipant.class.getName() + "/compensate",
                compensate.uri().toASCIIString());
        Assertions.assertEquals(ProxyParticipant.class.getName(), compensate.participantId());
        Assertions.assertNull(callbacks.callback(CallbackType.STATUS));
    }

    @Test
    public void writesTheParticipantsOwnEndpoints() {
        ParticipantCallbacks callbacks = resolve(JaxRsParticipant.class);

        LegacyHttpAddress compensate = (LegacyHttpAddress) callbacks.callback(CallbackType.COMPENSATE);
        Assertions.assertEquals(BASE_URI + "/work/compensate", compensate.uri().toASCIIString());
        Assertions.assertEquals("PUT", compensate.method());
        Assertions.assertEquals("ACTIVE", compensate.contextType());

        LegacyHttpAddress status = (LegacyHttpAddress) callbacks.callback(CallbackType.STATUS);
        Assertions.assertEquals(BASE_URI + "/work/status", status.uri().toASCIIString());
        Assertions.assertEquals("GET", status.method());
    }

    @Test
    public void writesAnEndpointDeclaredOnAnInterface() {
        ParticipantCallbacks callbacks = resolve(InterfaceCallbackParticipant.class);

        LegacyHttpAddress compensate = (LegacyHttpAddress) callbacks.callback(CallbackType.COMPENSATE);

        Assertions.assertEquals(BASE_URI + "/callback-interface/work/compensate",
                compensate.uri().toASCIIString());
    }

    @Test
    public void mixesEndpointAndProxyCallbacks() {
        ParticipantCallbacks callbacks = resolve(MixedParticipant.class);

        Assertions.assertInstanceOf(LegacyHttpAddress.class, callbacks.callback(CallbackType.COMPENSATE));
        Assertions.assertInstanceOf(HttpContractAddress.class, callbacks.callback(CallbackType.COMPLETE));
    }

    @Test
    public void buildsEveryAddressFromTheConfiguredBaseUri() {
        ParticipantCallbacks callbacks = resolve(MixedParticipant.class);

        Assertions.assertEquals(BASE_URI + "/work/compensate",
                callbacks.callback(CallbackType.COMPENSATE).callbackUri());
        Assertions.assertEquals(BASE_URI + "/lra-callback-proxy/" + MixedParticipant.class.getName() + "/complete",
                callbacks.callback(CallbackType.COMPLETE).callbackUri());
    }

    @Test
    public void rejectsAMissingBaseUri() {
        System.clearProperty(LRAConstants.NARAYANA_LRA_BASE_URI_PROPERTY_NAME);

        HttpAddressResolver resolver = new HttpAddressResolver();

        Assertions.assertThrows(IllegalStateException.class,
                () -> resolver.resolve(new MicroProfileLRAParticipant(ProxyParticipant.class)));
    }

    @Test
    public void writesTheProxyAddressesOfAContractParticipant() {
        ParticipantCallbacks callbacks = new HttpAddressResolver()
                .resolve(new LRAParticipant(ContractParticipant.class));

        Assertions.assertInstanceOf(HttpContractAddress.class, callbacks.callback(CallbackType.COMPENSATE));
        Assertions.assertEquals(BASE_URI + "/lra-callback-proxy/" + ContractParticipant.class.getName()
                + "/compensate", callbacks.callback(CallbackType.COMPENSATE).callbackUri());
        // a contract-shaped method is never the participant's own endpoint, even if it declares @Path
        Assertions.assertInstanceOf(HttpContractAddress.class, callbacks.callback(CallbackType.COMPLETE));
    }

    private static ParticipantCallbacks resolve(Class<?> javaClass) {
        return new HttpAddressResolver().resolve(new MicroProfileLRAParticipant(javaClass));
    }

    @jakarta.ws.rs.Path("/work")
    public static class JaxRsParticipant {
        @jakarta.ws.rs.PUT
        @jakarta.ws.rs.Path("compensate")
        @org.eclipse.microprofile.lra.annotation.Compensate
        public org.eclipse.microprofile.lra.annotation.ParticipantStatus compensate(URI lraId) {
            return org.eclipse.microprofile.lra.annotation.ParticipantStatus.Compensated;
        }

        @jakarta.ws.rs.GET
        @jakarta.ws.rs.Path("status")
        @org.eclipse.microprofile.lra.annotation.Status
        public org.eclipse.microprofile.lra.annotation.ParticipantStatus status(URI lraId) {
            return org.eclipse.microprofile.lra.annotation.ParticipantStatus.Compensated;
        }
    }

    public static class ProxyParticipant {
        @org.eclipse.microprofile.lra.annotation.Compensate
        public org.eclipse.microprofile.lra.annotation.ParticipantStatus compensate(URI lraId) {
            return org.eclipse.microprofile.lra.annotation.ParticipantStatus.Compensated;
        }
    }

    public interface CallbackInterface {

        @jakarta.ws.rs.PUT
        @jakarta.ws.rs.Path("/work/compensate")
        @org.eclipse.microprofile.lra.annotation.Compensate
        org.eclipse.microprofile.lra.annotation.ParticipantStatus compensate(URI lraId);
    }

    @jakarta.ws.rs.Path("/callback-interface")
    public static class InterfaceCallbackParticipant implements CallbackInterface {

        @Override
        public org.eclipse.microprofile.lra.annotation.ParticipantStatus compensate(URI lraId) {
            return org.eclipse.microprofile.lra.annotation.ParticipantStatus.Compensated;
        }
    }

    public static class MixedParticipant {
        @jakarta.ws.rs.PUT
        @jakarta.ws.rs.Path("/work/compensate")
        @org.eclipse.microprofile.lra.annotation.Compensate
        public org.eclipse.microprofile.lra.annotation.ParticipantStatus compensate(URI lraId) {
            return org.eclipse.microprofile.lra.annotation.ParticipantStatus.Compensated;
        }

        @org.eclipse.microprofile.lra.annotation.Complete
        public org.eclipse.microprofile.lra.annotation.ParticipantStatus complete(URI lraId) {
            return org.eclipse.microprofile.lra.annotation.ParticipantStatus.Completed;
        }
    }

    public static class ContractParticipant {
        @org.eclipse.microprofile.lra.annotation.Compensate
        public CompensateCallback.Reply compensate(CompensateCallback.Request request) {
            return null;
        }

        @jakarta.ws.rs.PUT
        @jakarta.ws.rs.Path("/contract/complete")
        @org.eclipse.microprofile.lra.annotation.Complete
        public CompleteCallback.Reply complete(CompleteCallback.Request request) {
            return null;
        }
    }
}
