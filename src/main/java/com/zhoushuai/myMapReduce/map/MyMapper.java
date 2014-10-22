package com.zhoushuai.myMapReduce.map;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(MyMapper.class);
	
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

	protected void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		String[] strs = value.toString().split(" ");
		Text keyy = new Text(strs[0]);
		IntWritable vall = new IntWritable(Integer.parseInt(strs[1]));
		context.write(keyy,vall);
	}

}
