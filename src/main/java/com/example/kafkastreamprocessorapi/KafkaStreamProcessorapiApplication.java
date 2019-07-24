package com.example.kafkastreamprocessorapi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Properties;

@SpringBootApplication
public class KafkaStreamProcessorapiApplication implements CommandLineRunner {

    @Bean
    KafkaProducer<String, Person> kafkaProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaProducer<String, Person> kafkaProducer = new KafkaProducer(p, Serdes.String().serializer(), new PersonSerializer());
        return kafkaProducer;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamProcessorapiApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        Topology topology = new Topology();
        Topology sourceTopology = topology.addSource("source", Serdes.String().deserializer(), new PersonDeserializer(), "topology-topic");
        Topology upperProcessor = sourceTopology.addProcessor("upper", () -> new PersonUpperProcessor(), "source");
        upperProcessor.addProcessor("peek", () -> new PeekProcessor(), "upper");

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        System.out.println(topology.describe());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();
    }
}
