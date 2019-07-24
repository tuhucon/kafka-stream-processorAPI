package com.example.kafkastreamprocessorapi;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class PersonUpperProcessor implements Processor<String, Person> {
    ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(String key, Person value) {
        value.setName(value.getName().toUpperCase());
        context.forward(key, value);
    }

    @Override
    public void close() {

    }
}
