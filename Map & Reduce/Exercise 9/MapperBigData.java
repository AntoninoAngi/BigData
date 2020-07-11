package it.polito.bigdata.hadoop.exercise7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type
    
	private MultipleOutputs<Text, NullWritable> mos = null;
	
	protected void setup (Context context) {
		mos = new MultipleOutputs<Text, NullWritable>(context);
	}
	
	
    protected void map(
    		LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");    
        float temperature = Float.parseFloat(fields[3]);
    	
        if (temperature > 30.0)
        	mos.write("highTemp", value, NullWritable.get());
        else
        	mos.write("normalTemp", value, NullWritable.get());
    	
    }
    
    protected void cleanup (Context context) throws IOException, InterruptedException {
		mos.close();
	}
    
}
