package it.polito.bigdata.hadoop.exercise12;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 12 - Mapper
 */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    FloatWritable> {// Output value type
    
	float threshold;
	
	protected void setup (Context context) {
		threshold = Float.parseFloat(context.getConfiguration().get("maxThreshold"));
	}
	
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		Float measure = Float.parseFloat(value.toString()); 
    		
    		if (measure < threshold) {
    			context.write(key, new FloatWritable(measure));
    		}
    }
}
