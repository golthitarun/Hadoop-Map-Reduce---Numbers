package JavaHDFS.JavaHDFS;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import JavaHDFS.JavaHDFS.MeanandVarience.Combiner;
import JavaHDFS.JavaHDFS.MeanandVarience.Map;
import JavaHDFS.JavaHDFS.MeanandVarience.Reduce;
import JavaHDFS.JavaHDFS.Top10MutualFriends.customComparator; 

public class MedianMinMax extends Configured implements Tool{
	private static int count = 0;
	public static class Map1 extends Mapper<LongWritable, Text, IntWritable, IntWritable>{										

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString().trim();
			int num = Integer.parseInt(line);
			int k = num/1000+1;

			context.write(new IntWritable(k), new IntWritable(num));
		}
	}


	public static class Reduce1 extends Reducer<IntWritable,IntWritable,Text, Text> {				
		public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			String s = "";
			int min = Integer.MAX_VALUE;
			int max = Integer.MIN_VALUE;
			int c = 0;
			String v = "";
			for(IntWritable i:values){
				c++;
				if(i.get()>max) {
					max = i.get();
				} else if(i.get()<min) {
					min = i.get();
				}
				v = v+ i.get()+":";
			}
			String k = key.get()+","+c+"\t";
			v = key.get()+","+min+","+max+","+c+","+v;
			context.write(new Text(k), new Text(v));// create a pair <keyword, number of occurrences>
			count = count + c;
		}
	}
	
	public static class Map2 extends Mapper<Text, Text, Text, Text>{
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			Text one = new Text("1");
			context.write(one, value);
		}
	}
	public static class Reduce2 extends Reducer<Text,Text,Text,Text> {
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		int mid = count/2;
		float median = 0;
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			boolean even = false;
			if(count%2 == 0) {
				even = true;
			}
						
			for(Text t : values) {	
				String[] k = t.toString().split(",");
				if(Integer.parseInt(k[1]) < min) {
					min = Integer.parseInt(k[1]);
				}

				if(Integer.parseInt(k[2]) > max) {
					max = Integer.parseInt(k[2]);
				}
				
				if(mid == -1) {
					continue;
				} else if(Integer.parseInt(k[3]) <= mid) {
					mid = mid - Integer.parseInt(k[3]);
				} else {
					String[] nums = k[4].split(":");
					Integer[] num = new Integer[nums.length];
					int i = 0;
					mid = nums.length - mid;
					for(String s : nums) {
						num[i++]= Integer.parseInt(s);
					}
					
					Arrays.sort(num);
					if(even && mid != 0) {
						median = num[mid-1] + num[mid];
						median = median/2;
					} else {
						median = num[mid-1];
					}
					mid = -1;
				}
			}
			
			String s = min+","+ max + "," + median;
			System.out.println(min + s);
			context.write(new Text("Min,Max,Median :"), new Text(s));
			
		}
	}
	public int run(String[] args) throws Exception{
		Configuration conf1 = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
	  	// get all args
	  	if (otherArgs.length != 2) {
	  	System.err.println("Usage: Friends <in> <out>");
	  	System.exit(2);
	  	}
	  	
	  	//Job1

		Job job1 = new Job(conf1, "MedianMinMax");
		job1.setJarByClass(MedianMinMax.class);
		job1.setJobName("job1");
	  	
		job1.setMapperClass(Map1.class);
	  	job1.setReducerClass(Reduce1.class);

	  	
	  	job1.setMapOutputKeyClass(IntWritable.class);
	    job1.setMapOutputValueClass(IntWritable.class);
	  	
	  	job1.setOutputKeyClass(Text.class);
	  	job1.setOutputValueClass(Text.class);
	  	

		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
	  	// set the HDFS path for the output
	  	FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]+ "/temp"));
	 
	  	job1.waitForCompletion(true);
	  	
	  	//Job2
	  
	  	Configuration conf2 = new Configuration();
	  	Job job2 = new Job(conf2);

	  	job2.setJarByClass(MedianMinMax.class);
		job2.setJobName("job2");

	  	job2.setMapperClass(Map2.class);
	  	job2.setReducerClass(Reduce2.class);

	  	job2.setNumReduceTasks(1);

	  	
	  //	job2.setInputFormatClass(SequenceFileInputFormat.class);
	  	job2.setInputFormatClass(TextInputFormat.class);
	  	job2.setOutputFormatClass(TextOutputFormat.class);


	/*  	job2.setOutputKeyClass(Text.class);
	  	job2.setOutputValueClass(IntWritable.class);*/
	  	
	  	
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setNumReduceTasks(1);
	    
	  	
		 FileInputFormat.setInputPaths(job2, new Path(otherArgs[1] + "/temp"));
		 FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+"/final"));


	  	//Wait till job completion
	  	//return job2.waitForCompletion(true) ? 0 : 1;
		FileSystem hdfs = FileSystem.get(conf1);
		Path t = new Path(otherArgs[1] + "/temp");
	  	if (job2.waitForCompletion(true)) {
	  		if (hdfs.exists(t)) {
		  	    hdfs.delete(t, true);
		  	}
	  		return 1;
	  	}
	  	else {
	  		return 0;
	  	}

	  	}	
	
		// Driver program
		public static void main(String[] args) throws Exception {
			int exitCode = ToolRunner.run(new MedianMinMax(), args);
			System.exit(exitCode);
		}

}
