package com.example.kafkastreamprocessorapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.json.JsonDeserializer;

import java.util.Map;

public class PersonDeserializer implements Deserializer<Person> {

    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Person deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Person.class);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    @Override
    public void close() {

    }
}
