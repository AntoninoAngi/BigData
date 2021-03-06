package it.polito.bigdata.spark.exercise36;

import org.apache.spark.api.java.*;

import scala.Tuple2;

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

		
		JavaRDD<String> PM10values = logRDD.filter(line -> {
			String[] fields = line.toString().split(",");
			double value = Double.parseDouble(fields[2]);
			
			if (value > 50)
				return true;
			else
				return false;
		});
		
		JavaPairRDD<String, String> values = PM10values.mapToPair(line -> {
			String[] fields = line.toString().split(",");
			Tuple2 <String, String> pair = new Tuple2 <String, String> (fields[0], fields[1]);
			
			return pair;
		});
		
		JavaPairRDD<String, Iterable<String>> finalSensorCriticalDates = values.groupByKey();

		finalSensorCriticalDates.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
