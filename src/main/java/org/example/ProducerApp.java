package org.example;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.constants.Constants;

import java.util.Properties;

@Getter
@Setter
public class ProducerApp {
    static int messageSent = 0;
    static int messageNotSent = 0;

    public static void produce(Properties props, Integer maxMessage) {
        ProducerApp producerApp = new ProducerApp();
        // Since we need to close our producer, we can use the try-with-resources statement to
        // create
        // a new producer
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            // here, we run an infinite loop to sent a message to the cluster every second
            for (int i = 0; i < maxMessage; i++) {
                String key = Integer.toString(i);
                String message = "this is message " + Integer.toString(i);

                producer.send(new ProducerRecord<String, String>(Constants.TOPIC, key, message));
                producerApp.increaseMessageSentCount();
                // log a confirmation once the message is written
                System.out.println("sent msg " + key);
                try {
                    // Sleep for a second
                    Thread.sleep(1000);
                } catch (Exception e) {
                    break;
                }
            }
        } catch (Exception e) {
            producerApp.increaseErrorCount();
            System.out.println("Could not start producer: " + e);
        }
    }

    public void increaseMessageSentCount() {
        this.messageSent += 1;
    }

    public void increaseErrorCount() {
        this.messageNotSent += 1;
    }
}
