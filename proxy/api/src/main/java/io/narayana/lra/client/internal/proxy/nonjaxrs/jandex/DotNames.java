/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.client.internal.proxy.nonjaxrs.jandex;

import io.narayana.lra.api.LRA;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.jboss.jandex.DotName;

public class DotNames {

    public static final DotName LRA = DotName.createSimple(org.eclipse.microprofile.lra.annotation.ws.rs.LRA.class.getName());
    public static final DotName COMPENSATE = DotName.createSimple(Compensate.class.getName());
    public static final DotName AFTER_LRA = DotName.createSimple(AfterLRA.class.getName());

    public static final DotName LRA_TRANSACTION = DotName.createSimple(LRA.Transaction.class.getName());
    public static final DotName LRA_COMPENSATE = DotName.createSimple(LRA.Compensate.class.getName());
    public static final DotName LRA_COMPLETE = DotName.createSimple(LRA.Complete.class.getName());
    public static final DotName LRA_STATUS = DotName.createSimple(LRA.Status.class.getName());
    public static final DotName LRA_FORGET = DotName.createSimple(LRA.Forget.class.getName());
    public static final DotName LRA_AFTER_LRA = DotName.createSimple(LRA.AfterLRA.class.getName());

    public static final DotName OBJECT = DotName.createSimple(Object.class.getName());
}