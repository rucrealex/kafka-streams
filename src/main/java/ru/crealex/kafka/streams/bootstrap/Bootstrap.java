package ru.crealex.kafka.streams.bootstrap;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.crealex.kafka.streams.model.Title;
import ru.crealex.kafka.streams.utility.JsonPOJOSerializer;

import java.util.Properties;

@Component
@Slf4j
public class Bootstrap {

    @EventListener
    public void onApplicationEvent(ContextRefreshedEvent event) {
        log.info(event.getApplicationContext().getApplicationName() + " started.");

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "titles1");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonPOJOSerializer.class);
        properties.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, JsonPOJOSerializer.class);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Title> titles = builder.stream("titles", Consumed.with(Serdes.String(), new JsonPOJOSerializer<>()));
        final KStream<String, Title> workTimes = builder.stream("times", Consumed.with(Serdes.String(), new JsonPOJOSerializer<>()));

        titles.filter((key, value) -> value != null)
                .foreach((key, value) -> log.info(String.valueOf(value)));

        titles.filter((key, value) -> value != null)
              .mapValues(value -> value.getName()).to("titles-output");


        Topology topology = builder.build();
        log.debug(topology.describe().toString());

        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();
    }
}
