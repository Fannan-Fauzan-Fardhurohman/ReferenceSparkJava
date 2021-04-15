package org.fanxan.rdd.readfileintordd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadFileTextRdd {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("sss");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String path = "D:/tutorial/spark/text1.txt";
        JavaRDD<String> lines = sc.textFile(path);

        for (String line:lines.collect()){
            System.out.println("* "+line);
        }
    }
}
