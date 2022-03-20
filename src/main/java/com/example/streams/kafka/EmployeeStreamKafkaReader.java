package com.example.streams.kafka;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import com.example.common.Employee;
import com.example.common.EmployeeStreamKafkaProducer;

public class EmployeeStreamKafkaReader {
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_RESET = "\u001B[0m";

    public static void main(String[] args) {
        try {
            final StreamExecutionEnvironment streamEnv =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            streamEnv.setParallelism(1);

            // Source Kafka consumer,
            // topic: employee
            Properties srcProps = new Properties();
            srcProps.setProperty("bootstrap.servers", "localhost:9092");
            srcProps.setProperty("group.id", "flink.streaming");

            FlinkKafkaConsumer<String> kafkaConsumer =
                    new FlinkKafkaConsumer<>("employee", // topic
                        new SimpleStringSchema(),
                        srcProps);  // connection props
            kafkaConsumer.setStartFromLatest();

            // Setting the source of the stream to kafka consumer
            DataStream<String> employeeStr =
                    streamEnv.addSource(kafkaConsumer);


            // Transform 1: Map each line to employee POJO
            DataStream<Employee> employee = employeeStr
                    .map(new MapFunction<String, Employee>() {
                        @Override
                        public Employee map(String s) throws Exception {
                            Employee emp = new Employee(s);
                            System.out.println(ANSI_GREEN + "emp: "+ emp + ANSI_RESET);
                            return emp;
                        }
                    });


            /**
             *  Stream Data Generator
             */
            Thread dataGenerator = new Thread(new EmployeeStreamKafkaProducer());
            dataGenerator.start();

            /**
             * Start Flink job
             */
            streamEnv.execute();
        }
        catch (Exception exp) {
            exp.printStackTrace();
        }
    }
}
