package com.example.kafkastreamprocessorapi;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class PeekProcessor implements Processor<String, Person> {
    ProcessorContext context;
    String storeName;
    KeyValueStore<String, Long> store;

    public PeekProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        store = (KeyValueStore<String, Long>) context.getStateStore(storeName);
    }

    @Override
    public void process(String key, Person value) {
        String storeKey = value.getId().toString();
        Long current = store.get(storeKey);
        if (current == null) {
            current = 1L;
        } else {
            current++;
        }
        store.put(storeKey, current);
        System.out.println(value + " is meet " + current + " times");
    }

    @Override
    public void close() {

    }
}
