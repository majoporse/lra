package io.narayana.lra.callbacks;

import java.util.concurrent.TimeUnit;

/**
 * Sender contract for delivering participant callbacks over Kafka and
 * correlating the participant's reply by correlation id.
 * <p>
 * The concrete implementation lives in the coordinator module where the Kafka
 * dependencies are available; {@code KafkaCallback} itself stays transport
 * agnostic and resolves the implementation via CDI at call time.
 */
public interface KafkaCallbackSender {

    /**
     * The Kafka topic on which the coordinator receives the participant's reply
     * to a callback request. Each delivered request carries it as its reply
     * topic so the participant knows where to publish the reply.
     */
    String getReplyTopic();

    /**
     * Deliver a participant callback request to the given topic and wait for the
     * reply correlated with {@code correlationId}.
     *
     * @param topic the participant's listener topic
     * @param type the envelope message type (the callback operation name)
     * @param payload the request payload sent to the participant
     * @param correlationId the unique id identifying this request/reply pair
     * @param timeout how long to wait for the reply before giving up
     * @param unit the unit of {@code timeout}
     * @return the raw JSON reply body, or {@code null} if the reply did not
     *         arrive within the timeout
     * @throws InterruptedException if the calling thread is interrupted while
     *         waiting for the reply
     */
    String send(String topic, String type, Object payload, String correlationId, long timeout, TimeUnit unit)
            throws InterruptedException;
}
