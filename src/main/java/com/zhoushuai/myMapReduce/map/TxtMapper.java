package com.zhoushuai.myMapReduce.map;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TxtMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(TxtMapper.class);
	
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		
		LOG.info("-----------------hello zhoushuai setup method");
		super.setup(context);
	}

	protected void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		
		String[] strs = value.toString().split(" ");
		for(String str:strs){
			context.write(new Text(str),new IntWritable(1));
		}
		
		LOG.info("-----------------heiheihei");
		
	}

}
