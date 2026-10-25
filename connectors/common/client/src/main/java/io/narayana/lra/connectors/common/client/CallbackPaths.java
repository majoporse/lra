/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client;

/**
 * The participant protocol ids shared by the connectors.
 */
public final class CallbackPaths {

    public static final String HTTP = io.narayana.lra.connectors.common.config.CallbackConfig.HTTP;

    public static final String KAFKA = io.narayana.lra.connectors.common.config.CallbackConfig.KAFKA;

    private CallbackPaths() {
    }
}
