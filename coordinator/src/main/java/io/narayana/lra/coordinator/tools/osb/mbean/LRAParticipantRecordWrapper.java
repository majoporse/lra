/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.coordinator.tools.osb.mbean;

import com.arjuna.ats.arjuna.coordinator.AbstractRecord;
import com.arjuna.ats.arjuna.tools.osb.mbean.ActionBean;
import com.arjuna.ats.arjuna.tools.osb.mbean.LogRecordWrapper;
import com.arjuna.ats.arjuna.tools.osb.mbean.ParticipantStatus;
import io.narayana.lra.callbacks.HttpCallback;
import io.narayana.lra.callbacks.LRACallback;
import io.narayana.lra.coordinator.domain.model.LRAParticipantRecord;
import java.net.URI;

public class LRAParticipantRecordWrapper extends LogRecordWrapper implements LRAParticipantRecordWrapperMBean {

    public LRAParticipantRecordWrapper(ActionBean parent, AbstractRecord rec, ParticipantStatus listType) {
        super(parent, rec, listType, LRAParticipantRecordWrapper.class.getName());
    }

    @Override
    public URI getRecoveryURI() {
        return getParticipant().getRecoveryURI();
    }

    @Override
    public String getParticipantId() {
        return getParticipant().getParticipantId();
    }

    @Override
    public String getCompensator() {
        LRACallback compensateCallback = getParticipant().getCompensateCallback();
        return compensateCallback instanceof HttpCallback
                ? ((HttpCallback) compensateCallback).getUri()
                : compensateCallback != null ? compensateCallback.toJson() : null;
    }

    @Override
    public boolean hasAfterCallback() {
        return getParticipant().hasAfterCallback();
    }

    @Override
    public String getLRAStatus() {
        return getParticipant().getStatus().name();
    }

    /**
     * The LRA record class is saved as part of the LongRunningAction record
     * and is not necessary to activate it separately.
     * When the parent LRA record is loaded the participant is fine to go.
     */
    @Override
    public boolean activate() {
        return true;
    }

    private LRAParticipantRecord getParticipant() {
        return ((LRAParticipantRecord) rec);
    }
}
