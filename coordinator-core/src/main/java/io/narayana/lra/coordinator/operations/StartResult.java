/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.operations;

import java.util.UUID;

public record StartResult(UUID lraId, UUID parentId) {
}
