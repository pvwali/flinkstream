package com.example.streams.filer;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.example.common.Employee;
import com.example.common.EmployeeStreamCSVWriter;

public class EmployeeStreamTaggedByDept {
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

            // Transform 1: Employee POJO with dept based commission/bonus
            OutputTag<Tuple2<Employee, Integer>> rdDept =
                    new OutputTag<Tuple2<Employee, Integer>>("RDDept") {};
            OutputTag<Tuple3<Employee, Integer, Integer>> salesDept  =
                    new OutputTag<Tuple3<Employee, Integer, Integer>>("SalesDept") {};

            SingleOutputStreamOperator<Employee> outputStreamOperator =
                employeeStr.process(new ProcessFunction<String, Employee>() {
                    @Override
                    public void processElement(
                            String line,
                            ProcessFunction<String, Employee>.Context context,
                            Collector<Employee> collector) throws Exception {

                        Employee emp = new Employee(line);
                        System.out.println("emp: "+ emp);

                        // Calculate sales commission + bonus
                        Tuple3<Employee, Integer, Integer>  salesDeptEmp =
                                new Tuple3<Employee, Integer, Integer>(emp, Math.round(emp.getSalary() * 0.05f), 500);
                        // Calculate spl bonus for R&D
                        Tuple2<Employee, Integer>  rDDeptEmp =
                                new Tuple2<Employee, Integer>(emp, Math.round(emp.getSalary() * 0.10f));

                        if (emp.getDept().equals("Sales")) {
                            context.output(salesDept, salesDeptEmp);
                        } else if (emp.getDept().equals("R&D")) {
                            context.output(rdDept, rDDeptEmp);
                        } else {
                            collector.collect(emp);
                        }
                    }
                });

            // Transform 2: Combine Sales emp to generate salary
            DataStream<Tuple3<Employee, Integer, Integer>> salesEmployees = outputStreamOperator.getSideOutput(salesDept);

            DataStream<Employee> combinedStream = outputStreamOperator.connect(salesEmployees)
                .map(new CoMapFunction<Employee, Tuple3<Employee, Integer, Integer>, Employee>() {
                    @Override
                    public Employee map1(Employee employee) throws Exception {
                        return employee;
                    }

                    @Override
                    public Employee map2(Tuple3<Employee, Integer, Integer> empWithComm) throws Exception {
                        Integer salary = empWithComm.f0.getSalary() + empWithComm.f1 + empWithComm.f2;
                        return new Employee(
                                empWithComm.f0.getId(), empWithComm.f0.getfName(), empWithComm.f0.getlName(),
                                empWithComm.f0.getHireDate(), salary, empWithComm.f0.getDept(), empWithComm.f0.getTimestamp());
                    }
                });

            // Transform 3: Combine R&D emp to generate salary
            DataStream<Tuple2<Employee, Integer>> rdEmployees = outputStreamOperator.getSideOutput(rdDept);

            combinedStream = combinedStream.connect(rdEmployees)
                .map(new CoMapFunction<Employee, Tuple2<Employee, Integer>, Employee>() {
                    @Override
                    public Employee map1(Employee employee) throws Exception {
                        return employee;
                    }

                    @Override
                    public Employee map2(Tuple2<Employee, Integer> empWithBonus) throws Exception {
                        Integer salary = empWithBonus.f0.getSalary() + empWithBonus.f1;
                        return new Employee(
                                empWithBonus.f0.getId(), empWithBonus.f0.getfName(), empWithBonus.f0.getlName(),
                                empWithBonus.f0.getHireDate(), salary, empWithBonus.f0.getDept(), empWithBonus.f0.getTimestamp());
                    }
                });

            combinedStream.print();

            /**
             *  Stream Data Generator
             */
            Thread dataGenerator = new Thread(new EmployeeStreamCSVWriter());
            dataGenerator.start();

            /**
             * Start Flink job
             */
            streamEnv.execute();

        } catch(Exception exp) {
            exp.printStackTrace();
        }
    }
}
