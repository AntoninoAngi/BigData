package it.polito.bigdata.hadoop.exercise6;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 13 - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	float max = 0;
    	float min = 1000;
    	
        for (FloatWritable value : values) {
        	if (max < value.get()) {
        		max = value.get();
        	}
        	if (min > value.get()) {
        		min = value.get();
        	}
        }
    	
    	context.write(key, new Text ("max=" + max + "_min=" + min));
        
    }
}
