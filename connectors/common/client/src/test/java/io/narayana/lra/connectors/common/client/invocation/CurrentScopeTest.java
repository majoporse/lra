/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.invocation;

import io.narayana.lra.Current;
import io.narayana.lra.LRAConstants;
import java.net.URI;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CurrentScopeTest {

    private URI outer;
    private URI inner;

    @BeforeEach
    public void setUp() {
        Current.popAll();
        outer = LRAConstants.toLRAId(UUID.randomUUID());
        inner = LRAConstants.toLRAId(UUID.randomUUID());
    }

    @AfterEach
    public void tearDown() {
        Current.popAll();
        Current.clearAuthToken();
    }

    @Test
    public void activatesAnLraUntilRestored() {
        CurrentScope scope = CurrentScope.capture();
        scope.activate(inner);

        Assertions.assertEquals(inner, Current.peek());

        scope.restore();

        Assertions.assertNull(Current.peek());
    }

    @Test
    public void keepsTheEnclosingLra() {
        Current.push(outer, null);
        Current.addActiveLRACache(outer);

        try {
            CurrentScope scope = CurrentScope.capture();
            scope.activate(inner);

            Assertions.assertEquals(inner, Current.peek());

            scope.restore();

            Assertions.assertEquals(outer, Current.peek());
        } finally {
            Current.removeActiveLRACache(outer);
        }
    }

    @Test
    public void keepsTheAuthToken() {
        Current.setAuthToken("token");

        CurrentScope scope = CurrentScope.capture();
        scope.activate(inner);
        scope.restore();

        Assertions.assertEquals("token", Current.getAuthToken());
    }

    @Test
    public void ignoresANullLra() {
        CurrentScope scope = CurrentScope.capture();
        scope.activate(null);

        Assertions.assertNull(Current.peek());

        scope.restore();

        Assertions.assertNull(Current.peek());
    }

    @Test
    public void leavesNoActiveCacheEntry() {
        CurrentScope scope = CurrentScope.capture();
        scope.activate(inner);
        scope.restore();

        // an entry left in the active cache would make this push visible again
        Current.push(inner, null);

        Assertions.assertNull(Current.peek());
    }
}
