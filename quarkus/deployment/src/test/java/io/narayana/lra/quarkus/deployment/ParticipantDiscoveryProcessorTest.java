/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.quarkus.deployment;

import io.narayana.lra.PropagateToken;
import java.io.IOException;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.eclipse.microprofile.lra.annotation.ws.rs.Leave;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.Indexer;
import org.jboss.jandex.MethodInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ParticipantDiscoveryProcessorTest {

    @Test
    public void managesMethodsWithLraLeaveAndPropagateToken() throws IOException {
        Index index = index(ManagedResource.class, LeaveInterface.class, LeaveImplementation.class);

        Assertions.assertTrue(
                ParticipantDiscoveryProcessor.isManaged(method(index, ManagedResource.class, "transactional"), index));
        Assertions.assertTrue(ParticipantDiscoveryProcessor.isManaged(method(index, ManagedResource.class, "leave"), index));
        Assertions.assertTrue(ParticipantDiscoveryProcessor.isManaged(method(index, ManagedResource.class, "token"), index));
        Assertions.assertFalse(ParticipantDiscoveryProcessor.isManaged(method(index, ManagedResource.class, "plain"), index));
        Assertions.assertTrue(
                ParticipantDiscoveryProcessor.isManaged(method(index, LeaveImplementation.class, "leaveFromInterface"), index));
    }

    @Test
    public void ignoresClassLevelPropagateTokenOnItsOwn() throws IOException {
        Index index = index(ClassLevelToken.class);

        Assertions.assertFalse(ParticipantDiscoveryProcessor.isManaged(method(index, ClassLevelToken.class, "plain"), index));
    }

    private static Index index(Class<?>... classes) throws IOException {
        Indexer indexer = new Indexer();

        for (Class<?> type : classes) {
            indexer.indexClass(type);
        }

        return indexer.complete();
    }

    private static MethodInfo method(Index index, Class<?> type, String name) {
        for (MethodInfo method : index.getClassByName(DotName.createSimple(type.getName())).methods()) {
            if (method.name().equals(name)) {
                return method;
            }
        }

        throw new IllegalArgumentException(name);
    }

    public static class ManagedResource {

        @LRA
        public void transactional() {
        }

        @Leave
        public void leave() {
        }

        @PropagateToken
        public void token() {
        }

        public void plain() {
        }
    }

    public interface LeaveInterface {

        @Leave
        void leaveFromInterface();
    }

    public static class LeaveImplementation implements LeaveInterface {

        @Override
        public void leaveFromInterface() {
        }
    }

    @PropagateToken
    public static class ClassLevelToken {

        public void plain() {
        }
    }
}
