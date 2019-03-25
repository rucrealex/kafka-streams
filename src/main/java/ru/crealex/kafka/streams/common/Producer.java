package ru.crealex.kafka.streams.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.crealex.kafka.streams.serde.JsonSerde;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Producer<K, V> {
    private final org.apache.kafka.clients.producer.Producer<K, V> producer;
    private final String topic;

    Producer(String topic) {
        this.topic = topic;
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVER);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerde.USER_EVENT_SERDE.getClass().getName());
        producer = new KafkaProducer<>(properties);
    }

    void close() {
        producer.flush();
        producer.close(TimeUnit.SECONDS.toSeconds(20), TimeUnit.SECONDS);
    }

    void sendEvent(K key, V event) {
        try {
            ProducerRecord<K, V> timeRecord = new ProducerRecord<K, V> (topic, AppConfig.PARTITION_COUNT, key, event);
            RecordMetadata metadata = producer.send(timeRecord).get();
            log.info(String.valueOf(event));
            log.info("topic: " + metadata.topic() + " partition: " + metadata.partition() + " offset: " + metadata.offset() + " timestamp " + metadata.timestamp());
        } catch (InterruptedException | ExecutionException e) {
            log.error("ERROR while sending record ", e);
        }
    }

}
