package com.jcairns.persistence;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class KafkaPublisher {

    private static final String PROPERTIES = "producer.properties";

    private static final String TOPIC = "testtopic";

    private final KafkaProducer<String, String> producer;

    private int sequence = 0;

    public KafkaPublisher() {
        try (final InputStream props = ClassLoader.getSystemResourceAsStream(PROPERTIES)) {
            Properties properties = new Properties();
            properties.load(props);

            final StringSerializer keySerializer = new StringSerializer();
            final StringSerializer valueSerializer = new StringSerializer();

            producer = new org.apache.kafka.clients.producer.KafkaProducer(properties, keySerializer, valueSerializer);
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka producer", e);
        }
    }

    public void publishMessage() throws ExecutionException, InterruptedException {

        final Future<RecordMetadata> future = producer.send(new ProducerRecord<>(TOPIC, Integer.toString(sequence++)));

        future.get();
    }
}