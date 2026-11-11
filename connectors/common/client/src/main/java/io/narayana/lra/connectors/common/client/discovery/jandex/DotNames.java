/*
   Copyright The Narayana Authors
   SPDX-License-Identifier: Apache-2.0
 */

package io.narayana.lra.connectors.common.client.discovery.jandex;

import io.narayana.lra.PropagateToken;
import io.narayana.lra.connectors.common.client.interceptor.LraManaged;
import org.eclipse.microprofile.lra.annotation.AfterLRA;
import org.eclipse.microprofile.lra.annotation.Compensate;
import org.eclipse.microprofile.lra.annotation.Complete;
import org.eclipse.microprofile.lra.annotation.Forget;
import org.eclipse.microprofile.lra.annotation.Status;
import org.eclipse.microprofile.lra.annotation.ws.rs.Leave;
import org.jboss.jandex.DotName;

public class DotNames {

    public static final DotName LRA = DotName
            .createSimple(org.eclipse.microprofile.lra.annotation.ws.rs.LRA.class.getName());
    public static final DotName COMPENSATE = DotName.createSimple(Compensate.class.getName());
    public static final DotName COMPLETE = DotName.createSimple(Complete.class.getName());
    public static final DotName STATUS = DotName.createSimple(Status.class.getName());
    public static final DotName FORGET = DotName.createSimple(Forget.class.getName());
    public static final DotName AFTER_LRA = DotName.createSimple(AfterLRA.class.getName());
    public static final DotName LEAVE = DotName.createSimple(Leave.class.getName());
    public static final DotName PROPAGATE_TOKEN = DotName.createSimple(PropagateToken.class.getName());
    public static final DotName LRA_MANAGED = DotName.createSimple(LraManaged.class.getName());

    public static final DotName INTERCEPTOR = DotName.createSimple("jakarta.interceptor.Interceptor");
    public static final DotName OBJECT = DotName.createSimple(Object.class.getName());
}
