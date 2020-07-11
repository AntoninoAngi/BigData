package it.polito.bigdata.hadoop.exercise22;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 22 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
    
	private String name;
	
	protected void setup(Context context) {
		name = context.getConfiguration().get("friend_interested");
	}
	
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            
            if (fields[0].compareTo(name) == 0) {
            	context.write(NullWritable.get(), new Text (fields[1]));
            }else if (fields[1].compareTo(name) == 0) {
            	context.write(NullWritable.get(), new Text (fields[0]));
            }
            
            
            
    }
}




