package com.revature.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LiamReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		int total = 0;
		double average = 0;

		for (DoubleWritable value : values) {
			total += value.get();
			count++;
			average = total/count;
		}

		if(average > 30){
			context.write(key, new DoubleWritable(average));
		}

	}
}