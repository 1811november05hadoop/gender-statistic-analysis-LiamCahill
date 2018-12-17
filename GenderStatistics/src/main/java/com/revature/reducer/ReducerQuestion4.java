package com.revature.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerQuestion4 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		double difference = 0;
		double currentvalue = 0;

		for (DoubleWritable value : values) {
			double lastvalue = currentvalue;
			currentvalue = value.get();
			difference = currentvalue - lastvalue;
			
		}
		
		context.write(key, new DoubleWritable(difference));
	}
}