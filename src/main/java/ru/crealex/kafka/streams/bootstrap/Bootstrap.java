package ru.crealex.kafka.streams.bootstrap;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.crealex.kafka.streams.model.Title;
import ru.crealex.kafka.streams.model.TitleTime;
import ru.crealex.kafka.streams.model.WorkTime;
import ru.crealex.kafka.streams.utility.JsonPOJOSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class Bootstrap {

    @EventListener
    public void onApplicationEvent(ContextRefreshedEvent event) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "titles_app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonPOJOSerializer.class);
        properties.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, JsonPOJOSerializer.class);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Title> titles = builder.stream("titles", Consumed.with(Serdes.String(), new JsonPOJOSerializer<>(Title.class)));
        final KStream<String, WorkTime> workTimes = builder.stream("times", Consumed.with(Serdes.String(), new JsonPOJOSerializer<>(WorkTime.class)));

        KStream<String, WorkTime> worksTimesWithKey = workTimes
                .filter((key, workTime) -> workTime != null)
                .selectKey((key, workTime) -> String.valueOf(workTime.getTitleId()));

        worksTimesWithKey.filter((key, value) -> value != null)
                .foreach((key, value) -> log.debug(key + " " + String.valueOf(value)));

        KStream<String, Title> titlesWithKeys = titles
                .filter((key, title) -> title != null)
                .selectKey((key, title) -> String.valueOf(title.getId()));

        titlesWithKeys.filter((key, value) -> value != null)
                .foreach((key, value) -> log.debug(key + " " + String.valueOf(value)));

        KStream<String, TitleTime> joined = titlesWithKeys.leftJoin(worksTimesWithKey, (title, time) -> {
                    TitleTime titleTime = new TitleTime();
                    log.debug(String.valueOf(title));
                    titleTime.setId(title.getId());
                    titleTime.setName(title.getName());
                    if(time != null) {
                        titleTime.setSumHours(time.getHours());
                    } else {
                        titleTime.setSumHours(0L);
                    }

                    return titleTime;
                },
                JoinWindows.of(TimeUnit.SECONDS.toDays(7)),
                Joined.with(Serdes.String(), new JsonPOJOSerializer<>(Title.class), new JsonPOJOSerializer<>(WorkTime.class)));

        joined.foreach((key, value) -> log.debug(key + " " + String.valueOf(value)));

        joined.to("titles-output");

        Topology topology = builder.build();
        log.info(topology.describe().toString());

        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();
    }
}
