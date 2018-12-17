package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.LiamMapper;
import com.revature.map.MapperQuestion2;
import com.revature.map.MapperQuestion3;
import com.revature.map.MapperQuestion4;
import com.revature.reducer.LiamReducer;
import com.revature.reducer.ReducerQuestion2;
import com.revature.reducer.ReducerQuestion3;
import com.revature.reducer.ReducerQuestion4;




public final class Question4Driver {

	public static void main(String[] args) throws Exception {


		if (args.length != 2) {
			System.out.printf(
					"Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}

		
		Job job = new Job();

		
		job.setJarByClass(Question4Driver.class);

		
		job.setJobName("Average % change in female employment");

		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		

		job.setMapperClass(MapperQuestion4.class);
		job.setReducerClass(ReducerQuestion4.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
	
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}

