package com.zhoushuai.myMapReduce.job;


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import com.zhoushuai.myMapReduce.map.MyMapper;
import com.zhoushuai.myMapReduce.reduce.MyReducer;



public class MyCounter {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Path path = new Path("/tmp/bbb.txt");
		Path outFile = new Path("/tmp/zhoushuaibbb");
		Job job = new Job();
		job.setJarByClass(MyCounter.class);
		
		FileInputFormat.addInputPath(job, path);
		FileOutputFormat.setOutputPath(job, outFile);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(2);
//		job.setSortComparatorClass(MyComparator.class);
		
		job.waitForCompletion(true);
	}


}
