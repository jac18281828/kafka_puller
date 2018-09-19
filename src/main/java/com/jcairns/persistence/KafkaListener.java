package com.jcairns.persistence;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.function.Consumer;

public final class KafkaListener {

    private static final String PROPERTIES = "producer.properties";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer;

    KafkaListener() {
        try (final InputStream props = ClassLoader.getSystemResourceAsStream(PROPERTIES)) {
            Properties properties = new Properties();
            properties.load(props);
            consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
        } catch (IOException e) {
            throw new RuntimeException("Unable to start Kafka producer", e);
        }
    }


    public void poll() {

        final ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);

        if(!records.isEmpty()) {

            for (ConsumerRecord<String, String> record : records) {
                System.out.print(record.key());
                System.out.print(' ');
                System.out.print(record.value());
            }
        }
    }
}

