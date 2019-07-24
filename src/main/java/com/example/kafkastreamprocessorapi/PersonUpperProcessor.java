package com.example.kafkastreamprocessorapi;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

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
    }

    @Override
    public void process(String key, Person value) {
        value.setName(value.getName().toUpperCase());
        String storeKey = value.getId().toString();
        Long current = store.get(storeKey);
        if (current == null) {
            store.put(storeKey, 1L);
        } else {
            store.put(storeKey, current + 1L);
        }
        context.forward(key, value);
    }

    @Override
    public void close() {

    }
}
