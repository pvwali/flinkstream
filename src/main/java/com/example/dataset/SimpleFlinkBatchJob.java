package com.example.dataset;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class SimpleFlinkBatchJob {
    public static void main(String[] args) {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            List<String> fruits =
                    Arrays.asList("Apple", "Mango" , "Kiwi");

            DataSet<String> dataSetOfFruits =
                    env.fromCollection(fruits);


            System.out.println("Fruits Count: "+ dataSetOfFruits.count());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
