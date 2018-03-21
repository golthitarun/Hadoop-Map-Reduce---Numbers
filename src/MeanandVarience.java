package JavaHDFS.JavaHDFS;
import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Random;

public class MeanandVarience {
	
	public static int group = 0;
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
		private Text n = new Text();
		private IntWritable t = new IntWritable();
		Random rand = new Random();
		int random = rand.nextInt();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String number = value.toString();
			if (group==1000) {
				group = 0;
				random = rand.nextInt();
			}
			n.set(number);
			t.set(random);
			group++;
			context.write(t,n);	
		}		
	}
	public static class Combiner extends Reducer<IntWritable,Text,IntWritable,Text>{
		private Text store= new Text();
		private IntWritable t = new IntWritable();
		public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {			
			long sum=0;
			long squaresum=0;
			long count = 0;
			for(Text value:values){
				sum +=Long.parseLong(value.toString());
				squaresum +=Long.parseLong(value.toString())*Long.parseLong(value.toString());
				count ++;				
			}			
			store.set(Long.toString(sum)+":"+Long.toString(squaresum)+":"+Long.toString(count));
			t.set(1);
			context.write(t,store);
		}
	}
	public static class Reduce extends Reducer<IntWritable,Text,Text,Text>{
		
		public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			long sum = 0;
			long squaresum = 0;
			long count = 0;
		
			for(Text object:values) {
				
				String[] s = object.toString().split(":");
				sum +=Long.parseLong(s[0]);
				squaresum +=Long.parseLong(s[1]);
				count +=Long.parseLong(s[2]);
			}
			double mean = (double)sum / count;
			double varience = (squaresum - mean*mean*count)/count;
			//String sol = Double.toString(mean) + "\t"+ Double.toString(varience);
			context.write(new Text(Double.toString(mean)), new Text(Double.toString(varience)));

			
		}
	}
	
	public static void main(String[] args) throws Exception {
	  	Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	// get all args
	  	if (otherArgs.length != 2) {
	  	System.out.println(otherArgs[0]);
	  	System.err.println("Usage: MeanandVarience <in> <out>");
	  	System.exit(2);
	  	}
	  	// create a job with name "MeanandVarience"
	  	Job job = new Job(conf, "MeanandVarience");
	  	job.setJarByClass(MeanandVarience.class);
	  	job.setMapperClass(Map.class);
	  	job.setReducerClass(Reduce.class);
	  	job.setCombinerClass(Combiner.class);
	  	job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
	  	job.setOutputKeyClass(Text.class);
	  	// set output value type
	  	job.setOutputValueClass(Text.class);
	  	//set the HDFS path of the input data
	  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	  	// set the HDFS path for the output
	  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	  	//Wait till job completion
	  	System.exit(job.waitForCompletion(true) ? 0 : 1);
	  	}	
}
