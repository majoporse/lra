package io.narayana.lra.client;

import java.io.Closeable;
import java.net.URI;
import java.time.temporal.ChronoUnit;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public interface LRAClient extends Closeable {
    URI startLRA(URI parentLRA, String clientID, Long timeout, ChronoUnit unit, boolean verbose);

    void closeLRA(URI lraId, String compensator, String userData);

    void cancelLRA(URI lraId, String compensator, String userData);

    void leaveLRA(URI lraId, String body);

    URI joinLRA(URI lraId, Long timeLimit,
            URI compensateUri, URI completeUri,
            URI forgetUri, URI leaveUri, URI afterUri, URI statusUri,
            StringBuilder compensatorData);

    LRAStatus getStatus(URI lraId);

    void setCurrentLRA(URI lraId);
}
