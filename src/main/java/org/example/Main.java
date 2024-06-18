package org.example;

import org.example.constants.Constants;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {

        // Create configuration options for our consumer
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Constants.BOOTSTRAP_SERVERS);
        // The group ID is a unique identified for each consumer group
        props.setProperty("group.id", "my-group-id");
        // Since our producer uses a string serializer, we need to use the corresponding
        // deserializer
        props.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        // Every time we consume a message from kafka, we need to "commit" - that is, acknowledge
        // receipts of the messages. We can set up an auto-commit at regular intervals, so that
        // this is taken care of in the background
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");

        Thread consumerThread = new Thread(() -> ConsumerApp.consume(props));
        consumerThread.start();


        // Create configuration options for our producer and initialize a new producer
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", Constants.BOOTSTRAP_SERVERS);
        // We configure the serializer to describe the format in which we want to produce data into
        // our Kafka cluster
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Thread producerThread = new Thread(() -> ProducerApp.produce(producerProps,10));
        producerThread.start();
        System.out.println("Hello world!");
    }
}