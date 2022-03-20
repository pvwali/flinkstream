package com.example.streams.filer;

import java.io.File;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.FileUtils;

import com.example.common.Employee;
import com.example.common.EmployeeStreamCSVWriter;

public class EmployeeStreamCounter {
    public static void main(String[] args) {
        try {
            final StreamExecutionEnvironment streamEnv =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            streamEnv.setParallelism(3);

            // Source CSV
            String dataDir = "data/inp/emp";
            DataStream<String> employeeStr =  streamEnv.readFile(
                    new TextInputFormat(new Path(dataDir)),
                    dataDir,
                    FileProcessingMode.PROCESS_CONTINUOUSLY,
                    1000);

            // Transform 1: Map each line to employee POJO
            DataStream<Employee> employee = employeeStr
                    .map(new MapFunction<String, Employee>() {
                        @Override
                        public Employee map(String s) throws Exception {
                            Employee emp = new Employee(s);
                            System.out.println("emp: "+ emp);
                            return emp;
                        }
                    });

                    // Transform 2: Aggregate counts over timeWindow
                    DataStream<Tuple2<String, Integer>>  recordCounts = employee
                            .map(new MapFunction<Employee, Tuple2<String, Integer>>() {
                                @Override
                                public Tuple2<String, Integer> map(Employee employee) throws Exception {
                                    return new Tuple2<>(""+System.currentTimeMillis() ,
                                            new Integer(1));
                                }
                            })
                            .timeWindowAll(Time.seconds(5))
                            .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                                @Override
                                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                                    return new Tuple2<>(first.f0, first.f1+second.f1);
                                }
                            });


                        // Sink: Collect and write to file
                        String outDataDir = "data/out/emp";
                        FileUtils.cleanDirectory(new File(outDataDir));

                        final StreamingFileSink<Tuple2<String, Integer>> counterSink =
                                StreamingFileSink.forRowFormat(
                                        new Path(outDataDir),
                                        new SimpleStringEncoder<Tuple2<String, Integer>>("UTF-8")).build();
                        recordCounts.addSink(counterSink);
                        recordCounts.print();

            /**
             *  Stream Data Generator
             */
            Thread dataGenerator = new Thread(new EmployeeStreamCSVWriter());
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
