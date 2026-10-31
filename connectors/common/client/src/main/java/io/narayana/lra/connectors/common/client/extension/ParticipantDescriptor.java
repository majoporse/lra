/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.extension;

/**
 * One participant found by {@link ParticipantScanner}.
 *
 * @param className the participant class name
 * @param form the participant model that must be instantiated for it
 */
public record ParticipantDescriptor(String className, ParticipantForm form) {
}
