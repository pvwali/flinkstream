package com.example.streams.sql;

import java.io.File;
import java.sql.Timestamp;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import com.example.common.EmployeeStreamKafkaProducer;

public class EmployeeStreamKafkaSQLWithWindows {
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
                    .field("ts", Types.LONG)
                    .field("ProcessingTime", Types.SQL_TIMESTAMP).proctime();    // process time when taskMgr receives the event

            streamTblEnv.connect(kafkaDesc)
                    .withFormat(new Csv().deriveSchema())
                    .withSchema(empSchema)
                    .inAppendMode()
                    .registerTableSource("emp");

            Table empTbl = streamTblEnv
                    .sqlQuery("SELECT MAX(ProcessingTime), lName, count(*) FROM emp " +
//                            "GROUP BY TUMBLE(ProcessingTime, INTERVAL '5' SECOND), lName");    // tumbling window 5s
                            "GROUP BY HOP(ProcessingTime, INTERVAL '5' SECOND, INTERVAL '10' SECOND), lName");       // sliding window 10s with 5s overlap
            empTbl.printSchema();

            DataStream<Tuple3<Timestamp, String, Long>> recordCounts =
                streamTblEnv.toRetractStream(empTbl, Row.class)
                    .map(new MapFunction<Tuple2<Boolean, Row>, Tuple3<Timestamp, String, Long>>() {
                        @Override
                        public Tuple3<Timestamp, String, Long> map(Tuple2<Boolean, Row> retractRow) throws Exception {
                            Row row = retractRow.f1;

                            int i= 0;
                            Tuple3<Timestamp, String, Long> emp = new Tuple3<Timestamp, String, Long>(
                                    (Timestamp)row.getField(i++),
                                    (String)row.getField(i++),
                                    (Long)row.getField(i++));

                            System.out.println(ANSI_GREEN + "retract? " +retractRow.f0 + ", " + emp + ANSI_RESET);
                            return emp;
                        }
                    });

            // Write Aggregate to a fileSystem sync
            String outDataDir = "data/out/emp";
            FileUtils.cleanDirectory(new File(outDataDir));

            final StreamingFileSink<Tuple3<Timestamp, String, Long>> counterSink =
                    StreamingFileSink.forRowFormat(
                            new Path(outDataDir),
                            new SimpleStringEncoder<Tuple3<Timestamp, String, Long>>("UTF-8")).build();
            recordCounts.addSink(counterSink);
            recordCounts.print();

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
