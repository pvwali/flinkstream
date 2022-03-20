package com.example.streams.sql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class EmployeeSQL {
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_RESET = "\u001B[0m";

    public static void main(String[] args) {
        try {

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            BatchTableEnvironment
                    batchTblEnv = BatchTableEnvironment.create(env);

            TableSource empCSV = new CsvTableSource.Builder()
                    .path("data/emp.csv")
                    .field("EMPLOYEE_ID", Types.INT)
                    .field("FIRST_NAME", Types.STRING)
                    .field("LAST_NAME", Types.STRING)
                    .field("EMAIL", Types.STRING)
                    .field("PHONE_NUMBER", Types.STRING)
                    .field("HIRE_DATE", Types.STRING)
                    .field("JOB_ID", Types.STRING)
                    .field("SALARY", Types.INT)
                    .field("COMMISSION_PCT", Types.STRING)
                    .field("MANAGER_ID", Types.STRING)
                    .field("DEPARTMENT_ID", Types.INT)
                    .build();

            batchTblEnv.registerTableSource("employee", empCSV);

            Table empSQL  = batchTblEnv.sqlQuery("SELECT * FROM employee " +
                    "WHERE EMPLOYEE_ID > 200 ");
            batchTblEnv.toDataSet(empSQL, Row.class).print();

            System.out.println(" = = Group By = =");
            Table empSQLGroupBy  = batchTblEnv.sqlQuery("SELECT DEPARTMENT_ID, Count(*) AS EMP_COUNT FROM employee " +
                    "Group BY DEPARTMENT_ID " +
                    "Order BY DEPARTMENT_ID ");
            batchTblEnv.toDataSet(empSQLGroupBy, Row.class).print();

            TableSource deptCSV = new CsvTableSource.Builder()
                    .path("data/dept.csv")
                    .field("id", Types.INT)
                    .field("Name", Types.STRING)
                    .build();

            System.out.println(" = = = Department = = = ");
            batchTblEnv.registerTableSource("dept", deptCSV);

            Table deptSQL = batchTblEnv.sqlQuery("SELECT * FROM dept");
            batchTblEnv.toDataSet(deptSQL, Row.class).print();

            Table emp_deptJoinSQL = batchTblEnv.sqlQuery("SELECT EMPLOYEE_ID, id, Name " +
                    "FROM dept, employee " +
                    "where DEPARTMENT_ID = id ");
            batchTblEnv.toDataSet(emp_deptJoinSQL, Row.class).print();

        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }
}
