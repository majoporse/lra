/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.http.support;

import io.narayana.lra.connectors.common.callbacks.CallbackType;
import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import io.narayana.lra.connectors.http.address.LegacyHttpAddress;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LinkHeaderParserTest {

    @Test
    public void parsesTheLegacyRelations() {
        ParticipantCallbacks callbacks = LinkHeaderParser.parse(
                "<http://participant/compensate>; rel=\"compensate\", "
                        + "<http://participant/after>; rel=\"after\"");

        LegacyHttpAddress compensate = (LegacyHttpAddress) callbacks.callback(CallbackType.COMPENSATE);
        LegacyHttpAddress after = (LegacyHttpAddress) callbacks.callback(CallbackType.AFTER);

        Assertions.assertEquals("http://participant/compensate", compensate.uri().toASCIIString());
        Assertions.assertEquals("PUT", compensate.method());
        Assertions.assertEquals("ACTIVE", compensate.contextType());
        Assertions.assertEquals("ENDED", after.contextType());
        Assertions.assertNull(callbacks.callback(CallbackType.COMPLETE));
    }

    @Test
    public void expandsTheParticipantRelation() {
        ParticipantCallbacks callbacks = LinkHeaderParser.parse("<http://participant>; rel=\"participant\"");

        Assertions.assertEquals("http://participant/compensate",
                ((LegacyHttpAddress) callbacks.compensateCallback).uri().toASCIIString());
        Assertions.assertEquals("http://participant/complete",
                ((LegacyHttpAddress) callbacks.completeCallback).uri().toASCIIString());
        Assertions.assertEquals("http://participant",
                ((LegacyHttpAddress) callbacks.statusCallback).uri().toASCIIString());
        Assertions.assertEquals("http://participant",
                ((LegacyHttpAddress) callbacks.forgetCallback).uri().toASCIIString());
    }
}
