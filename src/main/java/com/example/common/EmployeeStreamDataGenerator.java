package com.example.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class EmployeeStreamDataGenerator {
    public static final String ANSI_RESET = "\u001B[0m";

    public void run() {
        try {

            List<String> fName = new ArrayList<>();
            fName.add("Karen");
            fName.add("Harry");
            fName.add("Bob");

            List<String> lName = new ArrayList<>();
            lName.add("Walter");
            lName.add("Potter");
            lName.add("McCaffe");

            List<String> dept = new ArrayList<>();
            dept.add("Sales");
            dept.add("R&D");
            dept.add("Eng");

            List<String> date = new ArrayList<>();
            date.add("23-AUG-09");
            date.add("13-APR-09");
            date.add("06-FEB-09");
            date.add("09-DEC-09");
            date.add("11-MAR-09");

            setUp();

            //Define a random number generator
            Random random = new Random();

            //Generate 100 sample audit records, one per each file
            for(int i=0; i < 25; i++) {
                //Capture current timestamp
                String currentTime = String.valueOf(System.currentTimeMillis());

                //Generate a random user
                String eid = ""+random.nextInt(100);
                String empfName = fName.get(random.nextInt(fName.size()));
                String empLName = lName.get(random.nextInt(lName.size()));
                String hireDt = date.get(random.nextInt(date.size()));
                String sal = ""+random.nextInt(10)*1000;
                String eDept = dept.get(random.nextInt(dept.size()));
                String eArrivalTimestamp = ""+System.currentTimeMillis();

                //Create a CSV Text array
                String[] csvText = { eid, empfName, empLName, hireDt, sal, eDept, eArrivalTimestamp};

                writeToStream(i, csvText);

                //Sleep for a random time ( 1 - 3 secs) before the next record.
                Thread.sleep(random.nextInt(2000) + 1);
            }
            close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    protected  abstract void setUp() throws Exception;
    protected abstract void writeToStream(int i, String[] csvText) throws Exception;
    protected abstract void close() throws IOException;
}

