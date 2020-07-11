package it.polito.bigdata.hadoop.exercise8;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 8 - Reducer
 */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                DoubleWritable,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<DoubleWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    		int num = 0;
    		double totIncome = 0;
    		
    		for (DoubleWritable value : values ) {
    			num++;
    			totIncome += value.get();
    		}
    		
    		context.write(key, new DoubleWritable ((double)totIncome/num));
        	
    }
}
