package org.example;

import org.apache.kafka.clients.producer.Producer;
import org.example.ProducerApp;
import org.example.constants.Constants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

public class ProducerAppTest {

    @Mock
    private Producer<String, String> producer;

    private ProducerApp producerApp;
    static Properties producerProps = new Properties();

    @BeforeAll
    public static void setUpProps(){

    }
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        producerApp = new ProducerApp();
    }

    @After
    public void tearDown() {
        // Reset the message counts
        ProducerApp.messageSent = 0;
        ProducerApp.messageNotSent = 0;
    }

    @Test
    public void testProduce_SendsMessagesSuccessfully() throws Exception {
        // Given
        Integer maxMessage = 5;
        producerProps.put("bootstrap.servers", Constants.BOOTSTRAP_SERVERS);
        // We configure the serializer to describe the format in which we want to produce data into
        // our Kafka cluster
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // When
        ProducerApp.produce(producerProps, maxMessage);

        // Then

        assertEquals(5, ProducerApp.messageSent);
        assertEquals(0, ProducerApp.messageNotSent);
    }


    @Test
    public void testIncreaseMessageSentCount_IncrementsCount() {
        // When
        producerApp.increaseMessageSentCount();

        // Then
        assertEquals(1, ProducerApp.messageSent);
    }
}