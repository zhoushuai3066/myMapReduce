package com.zhoushuai.myMapReduce.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.zhoushuai.myMapReduce.job.TxtCounter;
import com.zhoushuai.myMapReduce.map.TxtMapper;
import com.zhoushuai.myMapReduce.reduce.TxtReducer;




public class LocalTxtCounter extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception{
	   Tool tool = new LocalTxtCounter();
	   ToolRunner.run(tool, args);
	}

	public int run(String[] arg0) throws Exception {
		
		Configuration conf = getConf();
		System.out.println(conf);
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.address", "192.168.109.240:8032");
		Path path = new Path("/user/zhoushuai/input.txt");
		Path outFile = new Path("/user/zhoushuai/countResult");
		Job job = new Job(conf);
		job.setJarByClass(TxtCounter.class);
		
		FileInputFormat.addInputPath(job, path);
		FileOutputFormat.setOutputPath(job, outFile);
		
		job.setMapperClass(TxtMapper.class);
		job.setReducerClass(TxtReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
		return 0;
	}

}
