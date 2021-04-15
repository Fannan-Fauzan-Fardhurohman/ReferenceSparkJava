package org.fanxan.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PrintRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Print Element of RDD")
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("D:/tutorial/spark/text1.txt");

        for(String line:lines.collect()){
            System.out.println("* "+line);
        }
    }
}
