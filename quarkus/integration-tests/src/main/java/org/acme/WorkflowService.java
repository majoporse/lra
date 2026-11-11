/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package org.acme;

import io.narayana.lra.api.Result;
import io.narayana.lra.connectors.common.contracts.AfterLRACallback;
import io.narayana.lra.connectors.common.contracts.CompensateCallback;
import io.narayana.lra.connectors.common.contracts.CompleteCallback;
import jakarta.enterprise.context.ApplicationScoped;
import java.net.URI;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

@ApplicationScoped
public class WorkflowService {

    @LRA(value = LRA.Type.MANDATORY, end = false)
    public Result<String> work(URI lraId) {
        return Result.success("ok");
    }

    @Complete
    public CompleteCallback.Reply complete(CompleteCallback.Request request) {
        return new CompleteCallback.Reply(ParticipantStatus.Completed);
    }

    @Compensate
    public CompensateCallback.Reply compensate(CompensateCallback.Request request) {
        return new CompensateCallback.Reply(ParticipantStatus.Compensated);
    }

    @AfterLRA
    public AfterLRACallback.Reply after(AfterLRACallback.Request request) {
        return new AfterLRACallback.Reply();
    }
}
