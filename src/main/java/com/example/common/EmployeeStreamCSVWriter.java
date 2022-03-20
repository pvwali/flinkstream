package com.example.common;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

import com.opencsv.CSVWriter;

public class EmployeeStreamCSVWriter extends EmployeeStreamDataGenerator implements Runnable{
    public static final String ANSI_BLUE = "\u001B[34m";

    //Define the data directory to output the files
    public final String dataDir = "data/inp/emp";

    @Override
    protected void setUp() throws IOException {
        //Clean out existing files in the directory
        FileUtils.cleanDirectory(new File(dataDir));
    }

    @Override
    protected void writeToStream(int i, String[] csvText) throws IOException {
        //Open a new file for this record
        FileWriter auditFile = new FileWriter(dataDir
                + "/emp_" + i + ".csv");

        CSVWriter auditCSV = new CSVWriter(auditFile);

        //Write the audit record and close the file
        auditCSV.writeNext(csvText);

        System.out.println(ANSI_BLUE + "Employee Generator : Creating File : "
                + Arrays.toString(csvText) + ANSI_RESET);

        auditCSV.flush();
        auditCSV.close();
    }

    @Override
    protected void close() throws IOException {
    }
}
