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
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.crealex.kafka.streams.model.Title;
import ru.crealex.kafka.streams.model.TitleTime;
import ru.crealex.kafka.streams.model.UserActivity;
import ru.crealex.kafka.streams.model.WorkTime;
import ru.crealex.kafka.streams.utility.JsonPOJOSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class Bootstrap {

    @EventListener
    public void onApplicationEvent(ContextRefreshedEvent event) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "titles_app" + Math.abs(100000 * Math.random()));
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/work/tmp/kafka-stream");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonPOJOSerializer.class);
        properties.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, JsonPOJOSerializer.class);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, Title> titles = builder.table("titles", Consumed.with(Serdes.String(), new JsonPOJOSerializer<>(Title.class)));
        final KStream<String, WorkTime> times = builder.stream("times", Consumed.with(Serdes.String(), new JsonPOJOSerializer<>(WorkTime.class)));

//        KStream<String, WorkTime> worksTimesWithKey = workTimes
//                .filter((key, workTime) -> workTime != null)
//                .selectKey((key, workTime) -> String.valueOf(workTime.getTitleId()));

//        titles.filter((key, value) -> value != null)
//                .foreach((key, value) -> log.debug("prn key: " + key + ", value:" + String.valueOf(value)));


//        KStream<String, Title> titlesWithKeys = titles
//                .filter((key, title) -> title != null)
//                .selectKey((key, title) -> String.valueOf(title.getId()));

//        times.filter((key, value) -> value != null)
//                .foreach((key, value) -> log.debug("prn key: " + key + ", value:" + String.valueOf(value)));



        KStream<String, TitleTime> titleTimes = times.groupByKey()
                .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1)))
                .aggregate(TitleTime::new, ((key, value, aggregateTime) -> aggregateTime.add(value)),
                        Materialized.<String, TitleTime, WindowStore<Bytes, byte[]>>as("times-aggregates")
                                .withValueSerde(new JsonPOJOSerializer<>(TitleTime.class)))
                .toStream()
                .selectKey(new KeyValueMapper<Windowed<String>, TitleTime, String>() {
                    @Override
                    public String apply(Windowed<String> key, TitleTime value) {
                        return String.valueOf(value.getId());
                    }
                });

        //        titleTimes.to("times-output", Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class)));

        titleTimes.to("times-output");

        KStream<String, UserActivity> joined = titleTimes.leftJoin(titles, (titleTime, title) -> {
            UserActivity userActivity = new UserActivity();
            if (title != null) {
                userActivity.setName(title.getName());
            }
            userActivity.setId(titleTime.getId());
            userActivity.setHours(titleTime.getSumHours());
            return userActivity;
        });

        joined.to("titles-output", Produced.valueSerde(new JsonPOJOSerializer<>(UserActivity.class)));

//        titleTimes.filter((key, value) -> value != null)
//                .foreach((key, value) -> log.debug("prn key: " + key + ", value:" + String.valueOf(value)));


//        KStream<String, Title> titlesOutput = titles.groupByKey().reduce(
//                (aggVal, newVal) -> {
//                    log.debug("aggVal:" + aggVal);
//                    log.debug("newVal:" + newVal);
//                    return newVal;
//                }, Materialized.with(Serdes.String(), new JsonPOJOSerializer<>(Title.class))
//        ).toStream();
//
//        titlesOutput.to("titles-output2");

//        KTable<String, WorkTime> reduce = times.filter((key, value) -> value != null)
//                .groupByKey()
//                .reduce((value1, value2) -> {
//
//                });
//                .
//                .reduce((value1, value2) -> {
//                    WorkTime workTime = new WorkTime();
//                    if(value1 != null && value2 != null) {
//                        workTime.setHours(value1.getHours() + value2.getHours());
//                    } else {
//                        workTime.setHours(value1.getHours());
//                    }
//                    return workTime;
//                });
//        reduce.toStream().to("times-output");


//        KStream<String, TitleTime> joined = titles.join(times, (title, time) -> {
//                    log.debug("time: " + String.valueOf(time));
//                    log.debug("title: " + String.valueOf(title));
//                    if (title == null || time == null) {
//                        return null;
//                    }
//
//                    if (title.getId().equals(time.getTitleId())) {
//                        TitleTime titleTime = new TitleTime();
//                        titleTime.setId(title.getId());
//                        titleTime.setName(title.getName());
//                        titleTime.setRole(title.getRole());
//                        titleTime.setIsManager(title.getIsManager());
//                        titleTime.setSumHours(time.getHours());
//                        return titleTime;
//                    }
//                    return null;
//                }, JoinWindows.of(TimeUnit.MINUTES.toMillis(1)),
//                Joined.with(Serdes.String(), new JsonPOJOSerializer<>(Title.class), new JsonPOJOSerializer<>(WorkTime.class)));


//        worksTimesWithKey.join(titlesWithKeys, (time, title) -> {
//                    TitleTime titleTime = new TitleTime();
//                    log.debug(String.valueOf(time));
//                    log.debug(String.valueOf(title));
//                    return
//                    titleTime.setId(title.getId());
//                    titleTime.setName(title.getName());
//                    if(time != null) {
//                        titleTime.setSumHours(time.getHours());
//                    } else {
//                        titleTime.setSumHours(0L);
//                    }
//
//                    return titleTime;
//                },
//                JoinWindows.of(TimeUnit.SECONDS.toDays(7)),
//                Joined.with(Serdes.String(), new JsonPOJOSerializer<>(WorkTime.class), new JsonPOJOSerializer<>(Title.class)));

//        joined.foreach((key, value) -> log.debug("joined: " + key + " " + String.valueOf(value)));
//
//        joined.to("titles-output");

        Topology topology = builder.build();
        log.info(topology.describe().toString());

        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.cleanUp();
        streams.start();
        try {
            Thread.sleep(Duration.ofMinutes(10).toMillis());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            streams.close();
            System.exit(0);
        }
    }
}
