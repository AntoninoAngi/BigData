package it.polito.bigdata.hadoop.exercise14;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class MapperBigData extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				Text, // Output key type
				NullWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		String[] keys = key.toString().split("\\s+");
		
		for (String keey : keys) {
			String keeey = keey.toLowerCase();
			context.write(new Text (keeey), NullWritable.get());
		}
	}
}
