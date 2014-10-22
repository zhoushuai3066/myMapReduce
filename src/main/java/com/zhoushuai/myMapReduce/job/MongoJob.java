package com.zhoushuai.myMapReduce.job;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.zhoushuai.myMapReduce.map.MongoMap;

public class MongoJob{

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Path outFile = new Path("/tmp/zhoushuai");
		
		Configuration conf = new Configuration();
		
//        MongoConfigUtil.setInputURI(conf, "mongodb://192.168.109.240/registrar.in" );
//        MongoConfigUtil.setInputURI(conf, "mongodb://192.168.108.147/mkdb.muser_relation");
		MongoConfigUtil.setInputURI(conf, "mongodb://192.168.109.240/registrar.cdr");
		
		Job job = new Job(conf,"word count");
		
		job.setJarByClass(MongoJob.class);
		FileOutputFormat.setOutputPath(job, outFile);
		job.setMapperClass(MongoMap.class);
//		job.setMapperClass(FriendRecommendMap.class);
		
		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(MongoInputFormat.class);  
		job.setOutputFormatClass(TextOutputFormat.class );
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
