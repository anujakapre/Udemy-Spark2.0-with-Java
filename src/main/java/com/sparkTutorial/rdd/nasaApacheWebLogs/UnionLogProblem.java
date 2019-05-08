package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UnionLogProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
           take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */
    	Logger.getLogger("org").setLevel(Level.ERROR);
    	SparkConf conf = new SparkConf().setAppName("UnionLogProblem").setMaster("local[3]");
    	JavaSparkContext context = null;
    	try {
    	 context = new JavaSparkContext(conf);
    	
    	JavaRDD<String> julyLogs= context.textFile("in/nasa_19950701.tsv").filter(line -> (!line.contains("Host")&& !line.contains("bytes")));
    	JavaRDD<String> augustLogs= context.textFile("in/nasa_19950801.tsv").filter(line -> (!line.contains("Host")&& !line.contains("bytes")));
    	
    	JavaRDD<String> combinedLogs = julyLogs.union(augustLogs);
    	
    	JavaRDD<String> sampledLogs = combinedLogs.sample(true,0.1);
    	
    	sampledLogs.saveAsTextFile("out/sample_nasa_logs_problem.csv");
    	}catch (Exception e) {
    		System.out.println("Something went wrong:\n"+e.getMessage());	
		}finally {
			context.close();
		}
    	
    }
}
