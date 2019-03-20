package ru.crealex.kafka.streams.utility;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import ru.crealex.kafka.streams.model.Title;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class JsonPOJOSerializer<T> implements Serializer<T>, Deserializer<T>, Serde<T> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
//    private Class<T> clazz;

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            log.warn("topic:" + topic);
            return (T) OBJECT_MAPPER.readValue(data, Title.class);
        } catch (final IOException e) {
            log.error(e.getMessage());
            return null;
//            throw new DeserializationException(e);
        }
    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
//        clazz = (Class<T>) configs.get("JsonPOJOClass");
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {
    }
}
