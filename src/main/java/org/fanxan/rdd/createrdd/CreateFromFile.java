package org.fanxan.rdd.createrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CreateFromFile {
    public static void main(String[] args) {
        SparkConf sparkConf= new SparkConf().setAppName("Create From File").setMaster("local[2]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // path
        String path = "D:/tutorial/spark/text1.txt";

        JavaRDD<String> lines = sparkContext.textFile(path);

        for(String line:lines.collect()){
            System.out.println("* "+line);
        }
    }
}
