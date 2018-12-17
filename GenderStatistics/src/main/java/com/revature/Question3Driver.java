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
import com.revature.reducer.LiamReducer;
import com.revature.reducer.ReducerQuestion2;
import com.revature.reducer.ReducerQuestion3;




public final class Question3Driver {

	public static void main(String[] args) throws Exception {


		/*
		 * The expected command-line arguments are the paths containing
		 * input and output data. Terminate the job if the number of
		 * command-line arguments is not exactly 2.
		 */
		if (args.length != 2) {
			System.out.printf(
					"Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}

		/*
		 * Instantiate a Job object for your job's configuration.  
		 */
		Job job = new Job();

		
		job.setJarByClass(Question3Driver.class);

		/*
		 * Specify an easily-decipherable name for the job.
		 * This job name will appear in reports and logs.
		 */
		job.setJobName("Average % change in female");

		/*
		 * Specify the paths to the input and output data based on the
		 * command-line arguments.
		 */
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		/*
		 * Specify the mapper and reducer classes.
		 */

		job.setMapperClass(MapperQuestion3.class);
		//job.setReducerClass(ReducerQuestion3.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		
		/*
		 * Start the MapReduce job and wait for it to finish.
		 * If it finishes successfully, return 0. If not, return 1.
		 */
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}

