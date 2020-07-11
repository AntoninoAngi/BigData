package it.polito.bigdata.hadoop.exercise14;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 */
class ReducerBigData extends
		Reducer<Text, // Input key type
			NullWritable, // Input value type
				Text, // Output key type
				NullWritable> { // Output value type

	// The reduce method is called only once in this approach
	// All the key-value pairs emitted by the mappers have the
	// same key (NullWritable.get())
	@Override
	protected void reduce(Text key, // Input key type
			Iterable<NullWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		context.write(key, NullWritable.get());
		
	}
}
