package com.revature.map;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LiamMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

    /*
     * The map method runs once for each line of text in the input file. The method
     * receives a key of type LongWritable, a value of type Text, and a Context
     * object.
     */
    @Override
    public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

        /*
         * Convert the line, which is received as a Text object, to a String object.
         */
        String line = value.toString();

        /*
         * The line.split("\\W+") call uses regular expressions to split the line up by
         * non-word characters.
         */
        for (String word : line.split(",")) {
           BufferedWriter writer = new BufferedWriter( new FileWriter(new File("src/input/inter.txt")));
            		 context.write(new Text(word), new IntWritable(1));
            	
               
            
        }
    }

}