package org.fanxan.rdd.readMultipleFileToSingleRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TextFileExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("membaca file");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // provide path file
        String files = "D:/tutorial/spark/text1.txt,D:/tutorial/spark/text2.txt,D:/tutorial/spark/text3.txt";

        // read text files to rdd
        JavaRDD<String> lines = sc.textFile(files);

        for(String line:lines.collect()){
            System.out.println("* "+line);
        }
    }
}
