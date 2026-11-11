/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.contracts;

import io.narayana.lra.LRAData;
import io.narayana.lra.connectors.common.callbacks.CallbackResult;

/**
 * The contract form of {@link LRAData}: the domain data plus the callback outcome the coordinator
 * reported for the LRA.
 * <p>
 * The domain model stays free of callback types; this contract DTO owns them and is mapped at the
 * coordinator and participant boundaries. The JSON shape is the same as the domain data plus the
 * {@code callbackResult} property.
 */
public class LRAInfo extends LRAData {

    private CallbackResult callbackResult;

    public LRAInfo() {
    }

    private LRAInfo(LRAData data, CallbackResult callbackResult) {
        super(data.getLraId(), data.getClientId(), data.getStatus(), data.isTopLevel(), data.isRecovering(),
                data.getStartTime(), data.getFinishTime());
        this.callbackResult = callbackResult;
    }

    /**
     * Map domain data and its callback outcome to the contract form.
     */
    public static LRAInfo from(LRAData data, CallbackResult callbackResult) {
        return data == null ? null : new LRAInfo(data, callbackResult);
    }

    /**
     * Map the contract form back to the domain model.
     */
    public LRAData toLRAData() {
        return new LRAData(getLraId(), getClientId(), getStatus(), isTopLevel(), isRecovering(),
                getStartTime(), getFinishTime());
    }

    public CallbackResult getCallbackResult() {
        return callbackResult;
    }

    public void setCallbackResult(CallbackResult callbackResult) {
        this.callbackResult = callbackResult;
    }
}
