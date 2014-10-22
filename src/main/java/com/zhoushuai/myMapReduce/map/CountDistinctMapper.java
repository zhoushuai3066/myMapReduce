package com.zhoushuai.myMapReduce.map;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CountDistinctMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(CountDistinctMapper.class);
	
	private IntWritable v = new IntWritable(1);
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

	protected void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		
		String[] strs = value.toString().split(",");
		String str = strs[3];
		context.write(new Text(str),v);
		
		
	}

}
