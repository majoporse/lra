package io.narayana.lra.connectors.common.client;

import io.narayana.lra.connectors.common.callbacks.ParticipantCallbacks;
import java.io.Closeable;
import java.net.URI;
import java.time.temporal.ChronoUnit;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

/**
 * The participant's outbound handler for coordinator requests.
 * <p>
 * A participant service has exactly one implementation, selected by
 * {@code quarkus.lra.client.protocol}. The HTTP URI-based join overloads are convenience methods
 * on the HTTP client, not part of this interface.
 */
public interface LRAClient extends Closeable {

    /**
     * @return the participant protocol id this client implements ({@code "http"} or {@code "kafka"})
     */
    String protocolId();

    URI startLRA(URI parentLRA, String clientID, Long timeout, ChronoUnit unit, boolean verbose);

    void closeLRA(URI lraId, String compensator, String userData);

    void cancelLRA(URI lraId, String compensator, String userData);

    void leaveLRA(URI lraId, String body);

    /**
     * Enlist a participant using the connector-neutral callback address book.
     *
     * @param lraId the LRA to join
     * @param timeLimit how long the participant is prepared to wait for LRA completion
     * @param callbacks the participant's callback endpoints
     * @param previousParticipantData receives the participant data stored by an earlier enlistment
     * @param participantId the participant id
     * @return the recovery URL for this enlistment
     */
    URI enlistCompensator(URI lraId, Long timeLimit, ParticipantCallbacks callbacks,
            StringBuilder previousParticipantData, String participantId);

    LRAStatus getStatus(URI lraId);
}
