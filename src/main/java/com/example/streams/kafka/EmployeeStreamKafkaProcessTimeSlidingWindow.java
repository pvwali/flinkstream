package com.example.streams.kafka;

import java.util.Properties;
import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import com.example.common.Employee;
import com.example.common.EmployeeStreamKafkaProducer;

public class EmployeeStreamKafkaProcessTimeSlidingWindow {
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

            // ProcessingTime based Windows
            Random r = new Random();
            DataStream<Tuple5<Integer, Long, Long, Integer, Float>>  employeePurchasesWithInWindow =
                    employee.map(new MapFunction<Employee, Tuple5<Integer, Long, Long, Integer, Float>>() {
                        @Override
                        public Tuple5<Integer, Long, Long, Integer, Float> map(Employee employee) throws Exception {
                            int n = r.nextInt(50);
                            System.out.println(n + " @ "+ employee.getTimestamp());
                            return new Tuple5<Integer, Long, Long, Integer, Float>(
                                    1, employee.getTimestamp(), employee.getTimestamp(), n, 0f);  // employee purchases at cafe
                        }
                    })
//                    .timeWindowAll(Time.seconds(3))  // Tumbling window of size 3sec
//                    .timeWindowAll(Time.seconds(5), Time.seconds(2))  // Sliding window of size 5sec that slides by 2sec
                            .windowAll(SlidingProcessingTimeWindows.of( Time.seconds(5),Time.seconds(2)))
//                            .keyBy(0).window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                    .reduce((x,y) -> new Tuple5<Integer, Long, Long, Integer, Float>(
                            x.f0 + y.f0,   // No of employee checkouts
                            Math.min(x.f1, y.f1),  // Earliest checkout timestamp within the window
                            Math.max(x.f2, y.f2),  // Last checkout timestamp within the window
                            x.f3 + y.f3,   // Value of purchases
                            (float)(x.f3 + y.f3) / (x.f0+y.f0))  // Avg purchases
                    );

            employeePurchasesWithInWindow.print();

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
