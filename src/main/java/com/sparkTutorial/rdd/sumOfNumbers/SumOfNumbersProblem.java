package com.sparkTutorial.rdd.sumOfNumbers;

import java.util.Arrays;

import org.apache.commons.math3.primes.Primes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SumOfNumbersProblem {

	public static void main(String[] args) throws Exception {

		/*
		 * Create a Spark program to read the first 100 prime numbers from
		 * in/prime_nums.text, print the sum of those numbers to console.
		 * 
		 * Each row of the input file contains 10 prime numbers separated by spaces.
		 */
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("PrimeNumber").setMaster("local[1]");
		JavaSparkContext context = null;
		try {
			context = new JavaSparkContext(conf);
			JavaRDD<String> primes = context.textFile("in/prime_nums.text")
					.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator()).filter(line -> !line.isEmpty());
			
			JavaRDD<Integer> primeNumbers = primes.map(line -> Integer.valueOf(line));

			int sum = primeNumbers.reduce((x, y) -> x + y);
			System.out.println("Sum of prime numbers in file:"+sum);

		} catch (Exception exception) {
			System.out.println("Something went wrong" + exception.getMessage());
		} finally {
			context.close();
		}
	}
}
