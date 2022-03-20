package com.example.common;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class EmployeeStreamKafkaProducer extends EmployeeStreamDataGenerator implements Runnable {
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_PURPLE = "\u001B[35m";

    public final String topic = "employee";

    private Producer<String,String> myProducer;

    @Override
    protected void setUp() throws InterruptedException {
        System.out.println("Starting Kafka Generator..");
        Thread.sleep(5000);

        //Setup Kafka Client
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","localhost:9092");

        kafkaProps.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        myProducer = new KafkaProducer<String, String>(kafkaProps);
    }

    @Override
    protected void writeToStream(int i, String[] text) throws ExecutionException, InterruptedException {
        String value = String.join(",", text);

        int key = (int)Math.floor(System.currentTimeMillis()/1000);
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, String.valueOf(key), value);

        RecordMetadata rmd = myProducer.send(record).get();

        System.out.println(ANSI_PURPLE + "Employee Generator : Creating File : "
                + Arrays.toString(text) + ANSI_RESET);
    }

    @Override
    protected void close() throws IOException {
    }
}
