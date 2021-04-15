package org.fanxan.rdd.readMultipleFileToSingleRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadFolderExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Read Multiple File from folder").setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String files = "D:/tutorial/spark";

        JavaRDD<String> lines = sc.textFile(files);

//        collect rdd for printing
        for(String line:lines.collect()){
            System.out.println("* "+line);
        }
    }
}
