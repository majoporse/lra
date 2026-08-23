package io.narayana.lra.coordinator.api;

import io.narayana.lra.logging.LRALogger;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@ApplicationScoped
public class KafkaActionProducer {
    private static volatile KafkaActionProducer instance;

    private final KafkaProducer<String, String> producer;

    public KafkaActionProducer() {
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    @PostConstruct
    void init() {
        instance = this;
    }

    @PreDestroy
    void destroy() {
        instance = null;
    }

    public static KafkaActionProducer getInstance() {
        return instance;
    }

    /**
     * Send a message to a Kafka topic and wait for delivery confirmation.
     *
     * @param topic the Kafka topic
     * @param key the message key
     * @param value the message value
     * @return true if the message was delivered successfully, false otherwise
     */
    public boolean send(String topic, String key, String value) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record).get(30, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            LRALogger.logger.errorf("Failed to send Kafka message to topic %s: %s", topic, e.getMessage());
            return false;
        }
    }

    public void close() {
        producer.close();
    }
}
