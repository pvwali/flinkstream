package com.example.streams.filer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import com.example.common.Employee;
import com.example.common.EmployeeStreamCSVWriter;

public class EmployeeStreamSimpleFileReader {
    public static void main(String[] args) {
        try {
            final StreamExecutionEnvironment streamEnv =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            streamEnv.setParallelism(1);

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
