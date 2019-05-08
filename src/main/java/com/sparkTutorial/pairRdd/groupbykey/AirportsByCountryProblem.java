package com.sparkTutorial.pairRdd.groupbykey;

import java.util.Map.Entry;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.sparkTutorial.rdd.commons.Utils;

import scala.Tuple2;

public class AirportsByCountryProblem {

	public static void main(String[] args) throws Exception {

		/*
		 * Create a Spark program to read the airport data from in/airports.text, output
		 * the the list of the names of the airports located in each country.
		 * 
		 * Each row of the input file contains the following columns: Airport ID, Name
		 * of airport, Main city served by airport, Country where airport is located,
		 * IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST,
		 * Timezone in Olson format
		 * 
		 * Sample output:
		 * 
		 * "Canada", ["Bagotville", "Montreal", "Coronation", ...] "Norway" : ["Vigra",
		 * "Andenes", "Alta", "Bomoen", "Bronnoy",..] "Papua New Guinea", ["Goroka",
		 * "Madang", ...] ...
		 */
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("AirportsByCountry").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> airports = context.textFile("in/airports.text");

		JavaPairRDD<String, String> airportPair = airports.mapToPair(
				(PairFunction<String, String, String>) line -> new Tuple2<>(line.split(Utils.COMMA_DELIMITER)[3],
						line.split(Utils.COMMA_DELIMITER)[1]));
		JavaPairRDD<String, Iterable<String>> groupedRDD = airportPair.groupByKey();
		for (Entry<String, Iterable<String>> e : groupedRDD.collectAsMap().entrySet()) {
				System.out.println(e.getKey() + ":" + e.getValue());
		}

		context.close();

	}
}
