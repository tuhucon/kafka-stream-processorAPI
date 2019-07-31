package com.example.kafkastreamprocessorapi;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class ConsumerInterceptor implements org.apache.kafka.clients.consumer.ConsumerInterceptor<String, Person> {
    @Override
    public ConsumerRecords<String, Person> onConsume(ConsumerRecords<String, Person> records) {
        System.out.println(Thread.currentThread().getId() + " consumer interceptor: " + records);
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
