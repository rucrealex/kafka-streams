package ru.crealex.kafka.streams.bootstrap;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.crealex.kafka.streams.model.TimeEvent;
import ru.crealex.kafka.streams.model.UserEvent;
import ru.crealex.kafka.streams.model.UserSummary;
import ru.crealex.kafka.streams.repositories.TimeRepository;
import ru.crealex.kafka.streams.repositories.UserRepository;
import ru.crealex.kafka.streams.repositories.UserSummaryRepository;
import ru.crealex.kafka.streams.serde.JsonSerde;
import ru.crealex.kafka.streams.utility.JsonPOJOSerializer;

import java.util.Properties;

@Component
@Slf4j
public class Bootstrap {

    private final TimeRepository timeRepository;
    private final UserRepository userRepository;
    private final UserSummaryRepository userSummaryRepository;

    @Autowired
    public Bootstrap(TimeRepository timeRepository, UserRepository userRepository, UserSummaryRepository userSummaryRepository) {
        this.timeRepository = timeRepository;
        this.userRepository = userRepository;
        this.userSummaryRepository = userSummaryRepository;
    }

    @EventListener
    public void onApplicationEvent(ContextRefreshedEvent event) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "titles_app" + Math.round(1000 * Math.random()));
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/work/tmp/kafka-stream");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonPOJOSerializer.class);
        properties.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, JsonPOJOSerializer.class);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        final StreamsBuilder builder = new StreamsBuilder();
        log.info("Application " + properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + " started ...");

        final KTable<String, UserEvent> users = builder.table("users", Consumed.with(Serdes.String(), JsonSerde.USER_EVENT_SERDE));
        final KStream<String, TimeEvent> times = builder.stream("times", Consumed.with(Serdes.String(), JsonSerde.TIME_EVENT_SERDE));

        users.toStream().foreach((key, userEvent) -> {
            userRepository.save(userEvent);
        });

        times.foreach((key, timeEvent) -> {
            timeRepository.save(timeEvent);
        });

        KTable<String, TimeEvent> aggregated = times.groupByKey()
                .aggregate(TimeEvent::new, ((key, time, aggregate) -> aggregate.addTime(time)),
                        Materialized.<String, TimeEvent, KeyValueStore<Bytes, byte[]>>as("aggregated-store").withValueSerde(JsonSerde.TIME_EVENT_SERDE));

        KTable<String, UserSummary> joined =
                users.join(aggregated, (userEvent, timeEvent) -> new UserSummary(timeEvent.getHours(), userEvent),
                        Materialized.<String, UserSummary, KeyValueStore<Bytes, byte[]>>as("joined-store").withValueSerde(JsonSerde.USER_ACTITITY_SERDE));

        joined.toStream().peek((key, userSummary) -> {
            log.debug("save user summary: " + userSummary + " in database");
            userSummaryRepository.save(userSummary);
        }).to("users-output");

        Topology topology = builder.build();
        log.info(topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.debug("close streams ...");
            streams.close();
            log.debug("done!");
        }));

        streams.cleanUp();
        streams.start();
    }
}
