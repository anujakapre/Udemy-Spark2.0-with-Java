package com.sparkTutorial.pairRdd.sort;

import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SortedWordCountProblem {

	/*
	 * Create a Spark program to read the an article from in/word_count.text, output
	 * the number of occurrence of each word in descending order.
	 * 
	 * Sample output:
	 * 
	 * apple : 200 shoes : 193 bag : 176 ...
	 */
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName("SortedWorkCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> wordRDD = context.textFile("in/word_count.text").flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		System.out.println(wordRDD.first());
		JavaPairRDD<String,Integer> wordCountRDD = wordRDD.mapToPair(word->new Tuple2<String,Integer>(word, 1));
		
		System.out.println(wordCountRDD.first());
		JavaPairRDD<String,Integer> wordCountPair = wordCountRDD.reduceByKey((x,y)->x+y);

		System.out.println(wordCountPair.first());

		JavaPairRDD<Integer,String> countWordPair = wordCountPair.mapToPair(pair -> new Tuple2<Integer, String>(pair._2, pair._1));
		JavaPairRDD<Integer,String> sortedCountWordPair =countWordPair.sortByKey(false);
		
		System.out.println(sortedCountWordPair.first());
		
		JavaPairRDD<String,Integer> sortedWordCountRDD = sortedCountWordPair.mapToPair(pair->new Tuple2<String,Integer>(pair._2, pair._1));
		
		
		for (Tuple2<String, Integer> wordToCount : sortedWordCountRDD.collect()) {
            System.out.println(wordToCount._1() + " : " + wordToCount._2());
        }
		
		context.close();
		

	}

}
