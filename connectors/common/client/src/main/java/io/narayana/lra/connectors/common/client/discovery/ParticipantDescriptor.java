/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.discovery;

/**
 * One participant found by {@link ParticipantScanner}.
 *
 * @param className the participant class name
 */
public record ParticipantDescriptor(String className) {
}
