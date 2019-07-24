package com.example.kafkastreamprocessorapi;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

public class PersonUpperProcessor implements Processor<String, Person> {
    ProcessorContext context;
    String storeName;
    KeyValueStore<String, Long> store;

    public PersonUpperProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore<String, Long>)context.getStateStore(storeName);
        context.schedule(10_000L, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            KeyValueIterator<String, Long> iterator = store.range("needForward_", "needForward_z");
            List<KeyValue<String, Long>> xxx = new ArrayList<>();
            iterator.forEachRemaining(xxx::add);
            xxx.forEach(r -> {
                context.forward(r.key, r.value);
                store.delete(r.key);
            });
        });
    }

    @Override
    public void process(String key, Person value) {
        value.setName(value.getName().toUpperCase());
        String storeKey = value.getId().toString();
        Long current = store.get(storeKey);
        if (current == null) {
            current = 1L;
        } else {
            current++;
        }
        store.put(storeKey, current);
        store.put("needForward_" + storeKey, current);
    }

    @Override
    public void close() {

    }
}
