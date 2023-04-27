package org.example.employee.models.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;


public class GenericSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @SneakyThrows
    @Override
    public byte[] serialize(String topic, T data) {
        return objectMapper.writeValueAsBytes(data);
    }

    @SneakyThrows
    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return objectMapper.writeValueAsBytes(data);
    }

}
