package it.polito.bigdata.spark.exercise36;

import org.apache.spark.api.java.*;

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
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #36");

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

		
		JavaRDD<Double> PM10values = logRDD.map(line -> {
			String[] fields = line.toString().split(",");
			return new Double (fields[3]);
		});
		
		Double sum = PM10values.reduce((Double v1, Double v2) -> new Double (v1+v2));
		
		long numLines = PM10values.count();
		
		// Store the result in the output folder
		System.out.println((Double) sum/numLines);

		// Close the Spark context
		sc.close();
	}
}
