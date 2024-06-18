package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.ConsumerApp;
import org.example.constants.Constants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerAppTest {

    @Mock
    private KafkaConsumer<String, String> kafkaConsumer;

    @Mock
    private ConsumerRecords<String, String> consumerRecords;

    @Mock
    private ConsumerRecord<String, String> consumerRecord;
    @Mock
    private ConsumerApp consumerApp;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        consumerApp = new ConsumerApp();
    }

    @After
    public void tearDown() {
        // Reset the static counters
        ConsumerApp.totalMessage = 0;
        ConsumerApp.noOfError = 0;
    }

    /**
     *
     * for now this is just a demo as there is while(true) statement it will run continuously
     *
     * */
    @Test
    public void testConsume() throws Exception {
        // Set up the mock KafkaConsumer to return a mock ConsumerRecords
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);

        // Set up the mock ConsumerRecords to return a list of mock ConsumerRecord
        when(consumerRecords.iterator()).thenReturn(Arrays.asList(consumerRecord).iterator());

        // Set up the mock ConsumerRecord to return a value
        when(consumerRecord.value()).thenReturn("test message");

        // Call the consume method
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
        consumerApp.consume(props);

        // Verify that the increaseMessageSentCount method was called
        verify(consumerApp).increaseMessageSentCount();

        // Verify that the totalMessage counter was incremented
        assertEquals(1, ConsumerApp.totalMessage);
    }

}