package com.example.streams.sql;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import com.example.common.EmployeeStreamKafkaProducer;

public class EmployeeStreamKafkaRetractSQL {
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

            Table empTbl = streamTblEnv.sqlQuery("SELECT lName, count(*) as lcnt from emp group by lName");
            empTbl.printSchema();

            streamTblEnv.toRetractStream(empTbl, Row.class)
                .map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Tuple2<Boolean, Row> retractRow) throws Exception {
                        Row row = retractRow.f1;

                        int i= 0;
                        Tuple2<String, Long> emp = new Tuple2<String, Long>(
                                (String)row.getField(i++),
                                (Long)row.getField(i++));

                        System.out.println(ANSI_GREEN + "retract? " +retractRow.f0 + ", " + emp + ANSI_RESET);
                        return emp;
                    }
                });


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
}
