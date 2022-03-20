package com.example.streams.filer;

import java.io.File;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.FileUtils;

import com.example.common.Employee;
import com.example.common.EmployeeStreamCSVWriter;

public class EmployeeStreamGroupedByLastNameGetCounts {
    public static final String ANSI_CYAN = "\u001B[36m";

    public static void main(String[] args) {
        try {
            final StreamExecutionEnvironment streamEnv =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            System.out.println("parallelism: "+streamEnv.getParallelism());

            // Source CSV
            String dataDir = "data/inp/emp";
            DataStream<String> employeeStr =  streamEnv.readFile(
                    new TextInputFormat(new Path(dataDir)),
                    dataDir,
                    FileProcessingMode.PROCESS_CONTINUOUSLY,
                    5000);


            // Transform 1: Map each line to employee POJO and group by LastName
            // Counts
            DataStream<Tuple2<String, Integer>> employeeLastNamesGroup = employeeStr
                    .map(new MapFunction<String, Employee>() {
                        @Override
                        public Employee map(String s) throws Exception {
                            Employee emp = new Employee(s);
                            System.out.println("emp: "+ emp);
                            return emp;
                        }
                    })
                    .map(e -> new Tuple2<String, Integer>(e.getlName(), 1))
                    .returns(Types.TUPLE(Types.STRING, Types.INT))
                    .keyBy(0)
                    .reduce((x,y) -> new Tuple2<String, Integer>(x.f0, x.f1 + y.f1));

            // Sink: Collect and write to file
            String outDataDir = "data/out/emp";
            FileUtils.cleanDirectory(new File(outDataDir));

            final StreamingFileSink<Tuple2<String, Integer>> counterSink =
                    StreamingFileSink.forRowFormat(
                            new Path(outDataDir),
                            new SimpleStringEncoder<Tuple2<String, Integer>>("UTF-8")).build();
            employeeLastNamesGroup.addSink(counterSink);
            employeeLastNamesGroup.print();

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
