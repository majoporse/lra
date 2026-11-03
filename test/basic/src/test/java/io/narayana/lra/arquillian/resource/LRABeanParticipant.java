/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.arquillian.resource;

import io.narayana.lra.connectors.common.contracts.AfterLRACallback;
import io.narayana.lra.connectors.common.contracts.CompensateCallback;
import io.narayana.lra.connectors.common.contracts.CompleteCallback;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.ParticipantStatus;
import org.eclipse.microprofile.lra.annotation.ws.rs.LRA;

/**
 * A plain (non-JAX-RS) CDI bean that participates in an LRA using the transport-neutral
 * annotations: {@code @LRA.Transaction} starts the LRA around the business method and
 * {@code @Compensate}/{@code @Complete}/{@code @AfterLRA} are the participant
 * callbacks the coordinator invokes (through the {@code lra-callback-proxy}) when the LRA
 * ends.
 */
@ApplicationScoped
public class LRABeanParticipant {

    private final AtomicInteger compensateCount = new AtomicInteger();
    private final AtomicInteger completeCount = new AtomicInteger();
    private final AtomicInteger afterCount = new AtomicInteger();
    private final List<String> afterStatuses = new CopyOnWriteArrayList<>();

    /**
     * Performs business work that succeeds; the LRA is closed when this method returns.
     */
    @LRA(LRA.Type.REQUIRED)
    public void doWork() {
        // pretend to do some work
    }

    /**
     * Performs business work that fails; the LRA is cancelled because the method throws.
     */
    @LRA(LRA.Type.REQUIRED)
    public void doFailingWork() {
        throw new IllegalStateException("the work failed, the LRA must be cancelled");
    }

    @Compensate
    public CompensateCallback.Reply compensate(CompensateCallback.Request request) {
        compensateCount.incrementAndGet();
        return new CompensateCallback.Reply(ParticipantStatus.Compensated);
    }

    @Complete
    public CompleteCallback.Reply complete(CompleteCallback.Request request) {
        completeCount.incrementAndGet();
        return new CompleteCallback.Reply(ParticipantStatus.Completed);
    }

    @AfterLRA
    public AfterLRACallback.Reply afterLRA(AfterLRACallback.Request request) {
        afterCount.incrementAndGet();
        afterStatuses.add(request.endStatus.name());
        return new AfterLRACallback.Reply();
    }

    public int compensateCount() {
        return compensateCount.get();
    }

    public int completeCount() {
        return completeCount.get();
    }

    public int afterCount() {
        return afterCount.get();
    }

    public List<String> afterStatuses() {
        return afterStatuses;
    }

    /**
     * Clears the invocation record; used by the tests to isolate the individual scenarios.
     */
    public void reset() {
        compensateCount.set(0);
        completeCount.set(0);
        afterCount.set(0);
        afterStatuses.clear();
    }
}
