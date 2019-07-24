package com.example.kafkastreamprocessorapi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {
    @Autowired
    KafkaProducer<String, Person> kafkaProducer;

    @GetMapping("/producer")
    public void producer(@RequestParam Integer count) {
        for (int i = 1; i <= count; i++) {
            Person p = new Person(i, "tu hu con " + i, i);
            ProducerRecord<String, Person> record = new ProducerRecord<>("topology-topic", p);
            kafkaProducer.send(record);
        }
    }
}
