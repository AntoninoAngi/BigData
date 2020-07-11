package it.polito.bigdata.spark.exercise36;

import org.apache.spark.api.java.*;

import scala.Tuple2;
import scala.collection.generic.BitOperations.Int;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #39");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		// Each element/string of the logRDD corresponds to one line of the
		// input file
		JavaRDD<String> logRDD = sc.textFile(inputPath);

		// Solution based on an named class
		// An object of the FilterGoogle is used to filter the content of the
		// RDD.
		// Only the elements of the RDD satisfying the filter imposed by means
		// of the call method of the FilterGoogle class are included in the
		// googleRDD RDD
		//JavaRDD<String> googleRDD = logRDD.filter(logLine -> logLine.toLowerCase().contains("google"));

		JavaRDD <String> PM10filtered = logRDD.filter(line -> {
			String[] fields = line.toString().split(",");
			if (Double.parseDouble(fields[2]) > 50)
				return true;
			else
				return false;
		});
		
		JavaPairRDD<String, Integer> PM10mapped = PM10filtered.mapToPair(line ->{
			String[] fields = line.toString().split(",");
			
			Tuple2<String, Integer> name = new Tuple2<String, Integer> (fields[0], new Integer (1));
			
			return name;
		});
		
		JavaPairRDD<String, Integer> PM10reduced = PM10mapped.reduceByKey((value1, value2) -> {
			return value1+value2;
		});
		
		JavaPairRDD<Integer, String> PM10mapped2 = PM10reduced.mapToPair((Tuple2<String, Integer> line)->{
			
			return new Tuple2 <Integer, String> (line._2(), line._1());
		});
		
		JavaPairRDD<Integer, String> PM10sorted = PM10mapped2.sortByKey();
		
		PM10sorted.saveAsTextFile(outputPath);
		

		// Close the Spark context
		sc.close();
	}
}
