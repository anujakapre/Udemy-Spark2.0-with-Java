package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.sparkTutorial.rdd.commons.Utils;

public class SameHostsProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */
    	Logger.getLogger("org").setLevel(Level.ERROR);
    	SparkConf conf = new SparkConf().setAppName("SameHostsApp").setMaster("local[1]");
    	JavaSparkContext context = null;
    	try {
    		context = new JavaSparkContext(conf);
    		JavaRDD<String> julyLogs= context.textFile("in/nasa_19950701.tsv").filter(line -> (!line.contains("Host")&& !line.contains("bytes"))).map(line -> {return line.split("\t")[0];});
        	JavaRDD<String> augustLogs= context.textFile("in/nasa_19950801.tsv").filter(line -> (!line.contains("Host")&& !line.contains("bytes"))).map(line -> {return line.split("\t")[0];});
        	        	
        	JavaRDD<String> newJavaRDD = julyLogs.intersection(augustLogs);
        	
        	newJavaRDD.saveAsTextFile("out/nasa_logs_same_hosts");
        	
    	}catch(Exception e) {
    		System.out.println("Something Went wrong"+e.getMessage());
    	}finally {
    		context.close();
    	}
    	
    
    }
}
