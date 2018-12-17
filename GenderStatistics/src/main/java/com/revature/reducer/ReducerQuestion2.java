package com.revature.reducer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerQuestion2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {



		ArrayList<Double> ForDiff = new ArrayList<Double>();
		int count = 0; 
		double currentvalue = 0;
		double sum = 0;

		for (DoubleWritable value : values) {


			if(count == 0){
				currentvalue = value.get();		
			}
			
			double lastvalue = currentvalue;
			currentvalue = value.get();
			double difference = currentvalue - lastvalue;
			ForDiff.add(difference);
			count++;
		}

		//use to iterate through the array of differences
		for(int i = 0;i < ForDiff.size();i++){
			sum += ForDiff.get(i);
		}
		
		double average = sum/ForDiff.size();

		context.write(key, new DoubleWritable(average));

		//still need function for calculating average and then writing it

	}
}