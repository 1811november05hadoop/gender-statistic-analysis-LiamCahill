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

public class MapperQuestion4 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		if(line.contains("SL.UEM.TOTL.FE.NE.ZS") && line.contains("United States")) {
			String[] fields = line.split(",");
			String country = fields[0];

			for(int i = 44; i < fields.length; i++){ 
				if(i == 44 || i == 60){
					try {
						double percent = Double.parseDouble(fields[i].replace("\"", ""));
						context.write(new Text(country), new DoubleWritable(percent));
					} catch(NumberFormatException e) {}
				}
			}
		}
	}
}