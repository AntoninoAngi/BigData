package it.polito.bigdata.hadoop.exercise7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	String invIndex = new String();
    	
        for (Text t : values) {
        	invIndex = invIndex.concat(t + ",");
        }
        	
        
        context.write(key, new Text (invIndex));	
        	
        	
    }
}
