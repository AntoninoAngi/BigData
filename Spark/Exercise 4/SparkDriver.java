package it.polito.bigdata.spark.exercise37;

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
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #37");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		// Each element/string of the logRDD corresponds to one line of the
		// input file
		JavaRDD<String> readingRDD = sc.textFile(inputPath);

		// Solution based on an named class
		// An object of the FilterGoogle is used to filter the content of the
		// RDD.
		// Only the elements of the RDD satisfying the filter imposed by means
		// of the call method of the FilterGoogle class are included in the
		// googleRDD RDD
		//JavaRDD<String> googleRDD = logRDD.filter(logLine -> logLine.toLowerCase().contains("google"));

		
		JavaPairRDD<String, Double> sensorsPM10ValuesRDD = readingRDD.mapToPair(
				PM10Reading -> {
					Double PM10value;
					String sensorID;
					Tuple2<String, Double> pair;

					// Split the line in fields
					String[] fields = PM10Reading.split(",");

					// fields[0] contains the sensorId
					sensorID = fields[0];

					// fields[2] contains the PM10 value
					PM10value = new Double(fields[2]);

					pair = new Tuple2<String, Double>(sensorID, PM10value);

					return pair;
		});
		
		JavaPairRDD<String, Double> sensorsMaxValuesRDD = sensorsPM10ValuesRDD.reduceByKey((value1, value2) -> {
			if (value1.compareTo(value2) > 0)
				return value1;
			else
				return value2;
		});

		sensorsMaxValuesRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
		
	}
}
