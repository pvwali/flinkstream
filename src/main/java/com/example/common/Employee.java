package com.example.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Employee {
    Integer id;
    String fName;
    String lName;
    Date hireDate;
    Integer salary;
    String dept;
    Long timestamp;

    public static String[] getHeader() {
        return "id,fName,lName,hireDate,salary,dept,timestamp".split(",");
    }

    public Employee(String line) {
        String[] record =
                line.replaceAll("\"", "").split(",");

        int i = 0;
        this.id = Integer.valueOf(record[i++]);
        this.fName = record[i++];
        this.lName = record[i++];
        try {
            this.hireDate = new SimpleDateFormat("dd-MMM-yy").parse(record[i++]);
        }
        catch (ParseException e) {
        }
        this.salary = Integer.valueOf(record[i++]);
        this.dept = record[i++];
        this.timestamp = Long.valueOf(record[i++]);
    }

    public Employee(Integer id, String fName, String lName, Date hireDate, Integer salary, String dept, Long ts) {
        this.id = id;
        this.fName = fName;
        this.lName = lName;
        this.hireDate = hireDate;
        this.salary = salary;
        this.dept = dept;
        this.timestamp = ts;
    }

    public Employee(String lName, Integer salary) {
        this.lName = lName;
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "id=" + id +
                ", fName='" + fName + '\'' +
                ", lName='" + lName + '\'' +
     //           ", hireDate=" + hireDate +
                ", salary=" + salary +
                ", dept=" + dept +
                ", ts=" + timestamp +
                '}';
    }

    public Integer getId() {
        return id;
    }

    public String getfName() {
        return fName;
    }

    public String getlName() {
        return lName;
    }

    public Date getHireDate() {
        return hireDate;
    }

    public Integer getSalary() {
        return salary;
    }

    public String getDept() { return dept; }

    public Long getTimestamp() {
        return timestamp;
    }
}
