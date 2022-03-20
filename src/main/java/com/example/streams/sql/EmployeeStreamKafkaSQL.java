package com.example.streams.sql;

import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import com.example.common.Employee;
import com.example.common.EmployeeStreamKafkaProducer;

public class EmployeeStreamKafkaSQL {
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_RESET = "\u001B[0m";

    public static void main(String[] args) {

        try {
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();

            StreamTableEnvironment
                    streamTblEnv = StreamTableEnvironment.create(env);

            Properties srcProps = new Properties();
            srcProps.setProperty("bootstrap.servers", "localhost:9092");
            srcProps.setProperty("group.id", "flink.streaming");

            ConnectorDescriptor kafkaDesc = new Kafka()
                    .version("universal")
                    .topic("employee")
                    .startFromLatest()
                    .properties(srcProps);

            Schema empSchema = new Schema()
                    .field("id", Types.INT)
                    .field("fName", Types.STRING)
                    .field("lName", Types.STRING)
                    .field("hireDate", Types.STRING)
                    .field("sal", Types.INT)
                    .field("dept", Types.STRING)
                    .field("ts", Types.LONG);

            streamTblEnv.connect(kafkaDesc)
                    .withFormat(new Csv().deriveSchema())
                    .withSchema(empSchema)
                    .inAppendMode()
                    .registerTableSource("emp");

//            Table empTbl = streamTblEnv.scan("emp");

//            Table empTbl = streamTblEnv.sqlQuery("SELECT * from emp");

//            Table empTbl = streamTblEnv.sqlQuery("SELECT * from emp");
//            printResultSet(streamTblEnv, empTbl);

            Table empTbl = streamTblEnv.sqlQuery("SELECT * from emp Where id > 80");
            printResultSet(streamTblEnv, empTbl);

            /**
             *  Stream Data Generator
             */
            Thread dataGenerator = new Thread(new EmployeeStreamKafkaProducer());
            dataGenerator.start();

            streamTblEnv.execute("");

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    private static void printResultSet(StreamTableEnvironment streamTblEnv, Table empTbl) {
        streamTblEnv.toAppendStream(empTbl, Row.class)
                .map(new MapFunction<Row, Employee>() {
                    @Override
                    public Employee map(Row row) throws Exception {
                        int i= 0;
                        Employee emp = new Employee(
                                (Integer)row.getField(i++),
                                (String)row.getField(i++),
                                (String)row.getField(i++),
                                new SimpleDateFormat("dd-MMM-yy").parse((String)row.getField(i++)),
                                (Integer)row.getField(i++),
                                (String)row.getField(i++),
                                (Long)row.getField(i++));
                        System.out.println(ANSI_GREEN + emp + ANSI_RESET);
                        return emp;
                    }
                });
    }
}
