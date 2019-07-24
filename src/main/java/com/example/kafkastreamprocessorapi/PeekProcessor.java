package com.example.kafkastreamprocessorapi;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class PeekProcessor implements Processor<String, Long> {
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
    public void process(String key, Long value) {
        System.out.println(String.format("%s: %d", key, value));
    }

    @Override
    public void close() {

    }
}
