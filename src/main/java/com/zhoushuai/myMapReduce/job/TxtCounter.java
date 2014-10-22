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

import com.zhoushuai.myMapReduce.map.TxtMapper;
import com.zhoushuai.myMapReduce.partitioner.TxtPartitioner;
import com.zhoushuai.myMapReduce.reduce.TxtReducer;



public class TxtCounter {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Path path = new Path("/tmp/aaa.txt");
		Path outFile = new Path("/tmp/zhoushuai");
		Job job = new Job();
		job.setJarByClass(TxtCounter.class);
		
		FileInputFormat.addInputPath(job, path);
		FileOutputFormat.setOutputPath(job, outFile);
		
		job.setMapperClass(TxtMapper.class);
		job.setReducerClass(TxtReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(2);
//		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setPartitionerClass(TxtPartitioner.class);
//		job.setSortComparatorClass(TxtComparator.class);
//		job.setGroupingComparatorClass(TxtComparator.class);
		
		
		job.waitForCompletion(true);
	}


}
