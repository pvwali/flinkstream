package com.example.streams.kafka;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import com.example.common.Employee;
import com.example.common.EmployeeStreamKafkaProducer;

public class EmployeeStreamKafkaEventTimeWindow {
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

            // EventTime based Windows with Periodic Watermarks
            Random r = new Random();
            DataStream<Tuple6<Integer, Long, Long, Integer, Float, Long>>  employeePurchasesWithInWindow =
                employee.map(new MapFunction<Employee, Tuple6<Integer, Long, Long, Integer, Float, Long>>() {
                    @Override
                    public Tuple6<Integer, Long, Long, Integer, Float, Long> map(Employee employee) throws Exception {
                        int n = r.nextInt(50);
                        System.out.println(n + " @ "+ employee.getTimestamp());
                        return new Tuple6<Integer, Long, Long, Integer, Float, Long>(
                                1, employee.getTimestamp(), employee.getTimestamp(), n, 0f, 0l);  // employee purchases at cafe
                    }
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple6<Integer, Long, Long, Integer, Float, Long>>() {
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(System.currentTimeMillis());
                    }

                    @Override
                    public long extractTimestamp(Tuple6<Integer, Long, Long, Integer, Float, Long> event, long l) {
                        return event.f1;     //Set EventTime based on event object
                    }
                }).timeWindowAll(Time.seconds(3))
                .reduce((x,y) -> new Tuple6<Integer, Long, Long, Integer, Float, Long>(
                        x.f0 + y.f0,   // No of employee checkouts
                        Math.min(x.f1, y.f1),  // Earliest checkout timestamp within the window
                        Math.max(x.f2, y.f2),  // Last checkout timestamp within the window
                        x.f3 + y.f3,   // Value of purchases
                        (float)(x.f3 + y.f3) / (x.f0+y.f0),
                        System.currentTimeMillis())  // Avg purchases
                );

            // Create a Kafka sink to drop the computed result
            FlinkKafkaProducer<String> kafkaProducer =
                new FlinkKafkaProducer<String>( "employeeRes", // topic
                        new SimpleStringSchema(), srcProps);

            employeePurchasesWithInWindow.map(
                    new MapFunction<Tuple6<Integer, Long, Long, Integer, Float, Long>, String>() {
                        @Override
                        public String map(Tuple6<Integer, Long, Long, Integer, Float, Long> res) throws Exception {

                            return String.join(",",
                                    String.valueOf(res.f0 +"|"+res.f1+"|"+res.f2+"|"+ res.f3+"|"+res.f4+"|"+ res.f5));
                        }
                    }).addSink(kafkaProducer);
            kafkaConsumer.setStartFromLatest();


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
