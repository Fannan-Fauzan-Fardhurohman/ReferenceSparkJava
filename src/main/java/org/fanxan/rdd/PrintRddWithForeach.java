package org.fanxan.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class PrintRddWithForeach {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Rdd Master");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        final JavaRDD<String> line = sc.textFile("D:/tutorial/spark/text1.txt");

        line.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("* "+s);
            }
        });
    }
}
