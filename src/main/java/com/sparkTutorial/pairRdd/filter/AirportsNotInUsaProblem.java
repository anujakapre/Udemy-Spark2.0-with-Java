package com.sparkTutorial.pairRdd.filter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.sparkTutorial.rdd.commons.Utils;

import scala.Tuple2;

public class AirportsNotInUsaProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text;
           generate a pair RDD with airport name being the key and country name being the value.
           Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located,
           IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "Canada")
           ("Wewak Intl", "Papua New Guinea")
           ...
         */
    	Logger.getLogger("org").setLevel(Level.ERROR);
    	SparkConf conf = new SparkConf().setAppName("AirportsNotInUSA").setMaster("local");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	JavaRDD<String> airportRDD = sc.textFile("in/airports.text");
    	
    	JavaPairRDD<String,String> pairedAiprotRDD = airportRDD.mapToPair(pairFunction());
    	
    	JavaPairRDD<String, String> filteredAirports = pairedAiprotRDD.filter(line -> !(line._2).equalsIgnoreCase("\"United States\""));
    	System.out.println(filteredAirports.first());
    
    	filteredAirports.saveAsTextFile("out/airports_not_in_usa_pair_rdd");
    }
    
    private static PairFunction<String, String, String> pairFunction(){
		
    	return (PairFunction<String, String, String>)  line -> new Tuple2<>(line.split(Utils.COMMA_DELIMITER)[1],line.split(Utils.COMMA_DELIMITER)[3]); 
//    	return null;
    	
    }
}
