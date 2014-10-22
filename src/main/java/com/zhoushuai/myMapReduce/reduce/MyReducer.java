package com.zhoushuai.myMapReduce.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, Text> {
	
	protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for(IntWritable it :values){
			sb.append(it);
			sb.append("_");
		}
		sb.append("]");
		context.write(key, new Text(sb.toString()));
	}
}
