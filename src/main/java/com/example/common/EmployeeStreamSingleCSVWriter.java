package com.example.common;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

import com.opencsv.CSVWriter;

public class EmployeeStreamSingleCSVWriter extends EmployeeStreamDataGenerator implements Runnable{
    public static final String ANSI_BLUE = "\u001B[34m";

    //Define the data directory to output the files
    public final String dataDir = "data/inp/emp";

    static CSVWriter auditCSV;

    @Override
    protected void setUp() throws IOException {
        //Clean out existing files in the directory
        FileUtils.cleanDirectory(new File(dataDir));
        //Open a new file for this record
        FileWriter auditFile = new FileWriter(dataDir
                + "/emp" + ".csv");

        auditCSV = new CSVWriter(auditFile);
        auditCSV.writeNext(Employee.getHeader());
        auditCSV.flush();
    }

    @Override
    protected void writeToStream(int i, String[] csvText) throws IOException {

        //Write the audit record and close the file
        auditCSV.writeNext(csvText);
        auditCSV.flush();

        System.out.println(ANSI_BLUE + "Employee Generator : Creating File : "
                + Arrays.toString(csvText) + ANSI_RESET);
    }

    @Override
    protected void close() throws  IOException {
        auditCSV.flush();
        auditCSV.close();
    }

    public static void main(String[] args) {
        /**
         *  CSV Data File Generator
         */
        Thread dataGenerator = new Thread(new EmployeeStreamSingleCSVWriter());
        dataGenerator.start();
    }
}
