/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.callbacks;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProtocolTest {

    @Test
    public void resolvesConfiguredValues() {
        Assertions.assertEquals(Protocol.HTTP, Protocol.from("http"));
        Assertions.assertEquals(Protocol.KAFKA, Protocol.from("kafka"));
    }

    @Test
    public void resolvesCaseInsensitivelyAndTrims() {
        Assertions.assertEquals(Protocol.HTTP, Protocol.from(" HTTP "));
        Assertions.assertEquals(Protocol.KAFKA, Protocol.from("KAFKA"));
    }

    @Test
    public void rejectsUnknownValues() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> Protocol.from("carrier-pigeon"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Protocol.from(null));
    }
}
