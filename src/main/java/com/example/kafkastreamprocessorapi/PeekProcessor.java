package com.example.kafkastreamprocessorapi;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class PeekProcessor implements Processor<String, Person> {
    ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(String key, Person value) {
        System.out.println(value);
    }

    @Override
    public void close() {

    }
}
