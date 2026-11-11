/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.discovery.cdi;

import io.narayana.lra.PropagateToken;
import java.lang.reflect.Method;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;
import org.eclipse.microprofile.lra.annotation.ws.rs.Leave;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LraBindingExtensionTest {

    @Test
    public void bindsMethodsWithLra() throws Exception {
        Assertions.assertTrue(LraBindingExtension.isManaged(method(ManagedResource.class, "transactional")));
    }

    @Test
    public void bindsMethodsWithLeave() throws Exception {
        Assertions.assertTrue(LraBindingExtension.isManaged(method(ManagedResource.class, "leave")));
    }

    @Test
    public void bindsMethodsWithPropagateToken() throws Exception {
        Assertions.assertTrue(LraBindingExtension.isManaged(method(ManagedResource.class, "token")));
    }

    @Test
    public void bindsLeaveDeclaredOnAnInterface() throws Exception {
        Assertions.assertTrue(
                LraBindingExtension.isManaged(method(LeaveImplementation.class, "leaveFromInterface")));
    }

    @Test
    public void ignoresUnannotatedMethods() throws Exception {
        Assertions.assertFalse(LraBindingExtension.isManaged(method(ManagedResource.class, "plain")));
    }

    @Test
    public void ignoresClassLevelPropagateTokenOnItsOwn() throws Exception {
        Assertions.assertFalse(LraBindingExtension.isManaged(method(ClassLevelToken.class, "plain")));
    }

    private static Method method(Class<?> type, String name) throws NoSuchMethodException {
        return type.getMethod(name);
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
