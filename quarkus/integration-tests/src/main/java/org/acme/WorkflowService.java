/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package org.acme;

import io.narayana.lra.api.LRA;
import io.narayana.lra.connectors.common.contracts.AfterLRACallback;
import io.narayana.lra.connectors.common.contracts.CompensateCallback;
import io.narayana.lra.connectors.common.contracts.CompleteCallback;
import jakarta.enterprise.context.ApplicationScoped;
import java.net.URI;

@ApplicationScoped
public class WorkflowService {

    @LRA.Transaction(value = LRA.Propagation.MANDATORY, end = false)
    public LRA.Result<String> work(URI lraId) {
        return LRA.Result.success("ok");
    }

    @LRA.Complete
    public CompleteCallback.Reply complete(CompleteCallback.Request request) {
        return new CompleteCallback.Reply(LRA.ParticipantOutcome.Completed);
    }

    @LRA.Compensate
    public CompensateCallback.Reply compensate(CompensateCallback.Request request) {
        return new CompensateCallback.Reply(LRA.ParticipantOutcome.Compensated);
    }

    @LRA.AfterLRA
    public AfterLRACallback.Reply after(AfterLRACallback.Request request) {
        return new AfterLRACallback.Reply();
    }
}
