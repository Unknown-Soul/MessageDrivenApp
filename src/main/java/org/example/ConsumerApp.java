package org.example;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.constants.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerApp {
    private static final Logger log = LoggerFactory.getLogger(ConsumerApp.class);
    public static int totalMessage = 0;
    public static int noOfError = 0;

    public static void consume(Properties props) {
        ConsumerApp consumerApp = new ConsumerApp();
        // Since we need to close our consumer, we can use the try-with-resources statement to
        // create it
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Subscribe this consumer to the same topic that we wrote messages to earlier
            consumer.subscribe(Arrays.asList(Constants.TOPIC));
            // run an infinite loop where we consume and print new messages to the topic
            while (true) {
                // The consumer.poll method checks and waits for any new messages to arrive for the
                // subscribed topic
                // in case there are no messages for the duration specified in the argument (1000 ms
                // in this case), it returns an empty list
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    consumerApp.increaseMessageSentCount();
                    System.out.printf("received message: %s\n", record.value());
                }
            }
        } catch (Exception e) {
            consumerApp.increaseErrorCount();
            log.error(e.getMessage());
        }
    }

    public void increaseMessageSentCount() {
        this.totalMessage += 1;
    }

    public void increaseErrorCount() {
        this.noOfError += 1;
    }
}
