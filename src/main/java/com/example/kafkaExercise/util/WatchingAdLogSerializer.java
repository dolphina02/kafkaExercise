package com.example.kafkaExercise.util;
import com.example.kafkaExercise.vo.WatchingAdLog;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class WatchingAdLogSerializer implements Serializer<WatchingAdLog> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, WatchingAdLog data) {
        try {
            if (data == null){
                return null;
            }
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SecurityException("Exception Occured");
        }
    }

    @Override
    public void close() {
    }

}
