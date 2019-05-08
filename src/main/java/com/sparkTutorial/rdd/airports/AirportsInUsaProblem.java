package com.sparkTutorial.rdd.airports;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.shell.Command;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.sparkTutorial.rdd.commons.Utils;

import scala.collection.mutable.StringBuilder;

public class AirportsInUsaProblem {

	public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
           and output the airport's name and the city's name to out/airports_in_usa.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "Putnam County Airport", "Greencastle"
           "Dowagiac Municipal Airport", "Dowagiac"
           ...
         */
    	
    	  Logger.getLogger("org").setLevel(Level.ERROR);
          SparkConf conf = new SparkConf().setAppName("USA-AIPRORTS").setMaster("local[3]");
          JavaSparkContext sc = new JavaSparkContext(conf);

          JavaRDD<String> airports = sc.textFile("in/airports.text");
          JavaRDD<String> details = airports.filter(line -> line.split(Utils.COMMA_DELIMITER)[3].equalsIgnoreCase("\"United States\""));
          System.out.println(details.first());
          
          JavaRDD<String> namesAndCities = details.map(line -> 
          { String splits[] = line.split(Utils.COMMA_DELIMITER);
          	return StringUtils.join(new String[]{splits[1],splits[2]},',');
          });
          System.out.println(namesAndCities.first());
          namesAndCities.saveAsTextFile("out/airports_in_usa_solution.text");
	}
}
