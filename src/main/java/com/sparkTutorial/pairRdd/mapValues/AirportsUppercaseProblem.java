package com.sparkTutorial.pairRdd.mapValues;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.sparkTutorial.rdd.commons.Utils;

import scala.Tuple2;

public class AirportsUppercaseProblem {

	public static void main(String[] args) throws Exception {

		/*
		 * Create a Spark program to read the airport data from in/airports.text,
		 * generate a pair RDD with airport name being the key and country name being
		 * the value. Then convert the country name to uppercase and output the pair RDD
		 * to out/airports_uppercase.text
		 * 
		 * Each row of the input file contains the following columns:
		 * 
		 * Airport ID, Name of airport, Main city served by airport, Country where
		 * airport is located, IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude,
		 * Timezone, DST, Timezone in Olson format
		 * 
		 * Sample output:
		 * 
		 * ("Kamloops", "CANADA") ("Wewak Intl", "PAPUA NEW GUINEA") ...
		 */
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("AirportNamesToUpperCase").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> airportList = context.textFile("in/airports.text");

		System.out.println(airportList);
		
		JavaPairRDD<String,String> airportNameCountryPair = airportList.mapToPair(line -> new Tuple2<String, String>(line.split(Utils.COMMA_DELIMITER)[1],line.split(Utils.COMMA_DELIMITER)[3].toUpperCase()));
		System.out.println(airportNameCountryPair.first());
		
		airportNameCountryPair.saveAsTextFile("out/airports_uppercase");
		context.close();
	}
}
