package com.zhoushuai.myMapReduce.map;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

public class MongoMap extends Mapper<Object, BSONObject, Text, IntWritable>{
	
protected void map(Object key,BSONObject value,Context context) throws IOException, InterruptedException{
		
	    String tdVal = (String) value.get("td");
	    String fromhost_ip = (String) value.get("fromhost-ip");
	    context.write(new Text(fromhost_ip), new IntWritable(Integer.parseInt(tdVal)));
//	    ClassDumperAgent classDumperAgent = new ClassDumperAgent();
	}

}
