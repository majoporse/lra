package io.narayana.lra.callbacks;

import io.narayana.lra.callbacks.contracts.kafka.KafkaCallbackReply;
import io.narayana.lra.callbacks.contracts.kafka.KafkaCallbackRequest;
import java.util.concurrent.TimeUnit;

public interface KafkaCallbackSender {

    String getReplyTopic();

    KafkaCallbackReply send(String topic, String type, KafkaCallbackRequest payload, String correlationId,
            long timeout, TimeUnit unit) throws InterruptedException;
}
