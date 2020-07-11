package it.polito.bigdata.spark.exercise30;

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
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #31");

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

		JavaRDD<Double> maxValues = logRDD.map(line -> {
				double maxValue = 0;
				String[] fields = line.toString().split(",");
				return new Double (fields[2]);
		});
		
		Double topValue = maxValues.reduce((value1, value2) -> {
			if (value1 > value2) {
				return value1;
			}else
				return value2;
		});
		
		JavaRDD<String> lines = logRDD.filter(line -> {
			String[] fields = line.toString().split(",");
			if (Double.parseDouble(fields[2]) == topValue) {
				return true;
			}else
				return false;
		});
		
		// Store the result in the output folder
		lines.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
