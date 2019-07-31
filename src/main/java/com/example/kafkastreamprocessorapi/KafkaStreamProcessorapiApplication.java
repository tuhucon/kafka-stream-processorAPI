package com.example.kafkastreamprocessorapi;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
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

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("tuhucon");
        StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Long());

        topology
                .addSource("source", Serdes.String().deserializer(), new PersonDeserializer(), "topology-topic")
                .addProcessor("upper", () -> new PersonUpperProcessor("tuhucon"), "source")
                .addProcessor("peek", () -> new PeekProcessor("tuhucon"), "upper");


        topology.addStateStore(storeBuilder, "upper", "peek");

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);

        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
                "com.example.kafkastreamprocessorapi.ConsumerInterceptor");


        System.out.println(topology.describe());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.setStateListener(((newState, oldState) -> {
            System.out.println("old State: " + oldState);
            System.out.println("new State: " + newState);
        }));
        kafkaStreams.start();
    }
}
