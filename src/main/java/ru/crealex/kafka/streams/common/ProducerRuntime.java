package ru.crealex.kafka.streams.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.crealex.kafka.streams.model.TimeEvent;
import ru.crealex.kafka.streams.model.UserEvent;
import ru.crealex.kafka.streams.serde.JsonSerde;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ProducerRuntime {
    final private String userTopic;
    final private String timeTopic;
    final private int partition = 0;

    final Producer<String, UserEvent> userProducer;
    final Producer<String, TimeEvent> timeProducer;

    public static void main(String[] args) throws InterruptedException {
        ProducerRuntime producerRuntime = new ProducerRuntime("users", "times");
        producerRuntime.sendTimes();
    }

    public ProducerRuntime(String userTopic, String timeTopic) {
        this.userTopic = userTopic;
        this.timeTopic = timeTopic;

        Properties userProperties = new Properties();
        userProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVER);
        userProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        userProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerde.USER_EVENT_SERDE.getClass().getName());

        Properties timeProperties = new Properties();
        timeProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVER);
        timeProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        timeProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerde.TIME_EVENT_SERDE.getClass().getName());

        userProducer = new KafkaProducer<String, UserEvent>(userProperties);
        timeProducer = new KafkaProducer<String, TimeEvent>(timeProperties);
    }

    public void sendUsers() throws InterruptedException {
        sendUser(1, AppConfig.TITLE_PRODUCT_OWNER, "Mark Simon", true, 0);
        sendUser(2, AppConfig.TITLE_TEAM_LEAD, "John Mitteran", true, 1);
        sendUser(3, AppConfig.TITLE_DEVELOPER, "Jim Morrison", false, 2);
        sendUser(4, AppConfig.TITLE_DEVELOPER, "Clark Kent", false, 3);
        sendUser(5, AppConfig.TITLE_DEVELOPER, "Jeff Kempinsky", false, 10);
        sendUser(6, AppConfig.TITLE_QA_ENGENEER, "Michel Limo", false, 11);
        sendUser(7, AppConfig.TITLE_DEVELOPER, "Rebecka Tanner", false, 12);

        userProducer.flush();
        userProducer.close(TimeUnit.SECONDS.toSeconds(20), TimeUnit.SECONDS);
    }

    public void sendTimes() throws InterruptedException {
        for (int i = 1; i < TimeUnit.SECONDS.toSeconds(4); i++) {
            for (int j = 1; j <= 7; j++) {
                sendTime(j, 1);
                Thread.sleep(TimeUnit.MILLISECONDS.toMillis(2000));
            }

            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException e) {
                timeProducer.flush();
                timeProducer.close(TimeUnit.SECONDS.toSeconds(20), TimeUnit.SECONDS);
                log.info("Send events interrupted");
            }
        }

        timeProducer.flush();
        timeProducer.close(TimeUnit.SECONDS.toSeconds(20), TimeUnit.SECONDS);
    }

    private void sendUser(long userId, String title, String name, boolean isManager, long sleepTime) throws InterruptedException {
        Thread.sleep(TimeUnit.SECONDS.toMillis(sleepTime));

        UserEvent userEvent = new UserEvent(userId, title, name, isManager);
        try {
            ProducerRecord<String, UserEvent> userRecord = new ProducerRecord<>(userTopic, partition, String.valueOf(userId), userEvent);
            RecordMetadata metadata = userProducer.send(userRecord).get();
            logRecordMetadata(metadata);
        } catch (InterruptedException | ExecutionException e) {
            log.error("ERROR while sending record ", e);
        }
    }

    private void sendTime(long userId, long hours) {
        TimeEvent timeEvent = new TimeEvent(userId, hours);
        try {
            ProducerRecord<String, TimeEvent> timeRecord = new ProducerRecord<>(timeTopic, partition, String.valueOf(userId), timeEvent);
            RecordMetadata metadata = timeProducer.send(timeRecord).get();
            logRecordMetadata(metadata);
        } catch (InterruptedException | ExecutionException e) {
            log.error("ERROR while sending record ", e);
        }
    }

    private void logRecordMetadata(RecordMetadata metadata) {
        log.info("topic: " + metadata.topic() +
                " partition: " + metadata.partition() +
                " offset: " + metadata.offset() +
                " timestamp " + metadata.timestamp());
    }
}
