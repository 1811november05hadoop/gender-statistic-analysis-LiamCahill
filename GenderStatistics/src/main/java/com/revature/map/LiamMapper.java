package com.revature.map;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LiamMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	/*
	 * The map method runs once for each line of text in the input file. The method
	 * receives a key of type LongWritable, a value of type Text, and a Context
	 * object.
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		/*
		 * Convert the line, which is received as a Text object, to a String object.
		 */
		String line = value.toString();


		if(line.contains("Educational attainment") && line.contains("female (%) (cumulative)")) {
			String[] fields = line.split(",");
			String country = fields[0];
			
			for(int i = 4; i < fields.length; i++){ 
				try {
					double percent = Double.parseDouble(fields[i].replace("\"", ""));
					context.write(new Text(country), new DoubleWritable(percent));
				} catch(NumberFormatException e) {}
			}
		}
	}
}