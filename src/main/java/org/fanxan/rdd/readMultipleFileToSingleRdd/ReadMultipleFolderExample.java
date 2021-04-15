package org.fanxan.rdd.readMultipleFileToSingleRdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadMultipleFolderExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("read multiple folder");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String dir = "D:/tutorial/spark,D:/tutorial/Spark2";

        JavaRDD<String> lines = sc.textFile(dir);

        for(String line:lines.collect()){
            System.out.println(line);
        }
    }
}
