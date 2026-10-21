/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.api;

/**
 * The protocol ids and the wire paths shared by the connectors.
 */
public final class CallbackPaths {

    /**
     * The HTTP path of the common callback contract: {@code {base}/lra-callback-proxy/{participantId}/{op}}.
     */
    public static final String RESOURCE_PATH = "lra-callback-proxy";

    public static final String HTTP = "http";

    public static final String KAFKA = "kafka";

    public static final String HTTP_CONTRACT = "http-contract";

    private CallbackPaths() {
    }
}
