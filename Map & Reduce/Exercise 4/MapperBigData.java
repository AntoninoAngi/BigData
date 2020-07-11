package it.polito.bigdata.hadoop.exercise7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.polito.bigdata.hadoop.exercise7.DriverBigData.MY_COUNTERS;

/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    NullWritable> {// Output value type
    
    protected void map(
    		LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            context.getCounter(MY_COUNTERS.TOTAL_RECORDS).increment(1);
    	
    	
    }
}
