package com.zhoushuai.myMapReduce.writableComparator;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class MyComparator extends WritableComparator {
	
	public MyComparator(){
		super(Text.class,true);
	}


	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Text atx = (Text) a;
		Text btx = (Text) b;
		return atx.compareTo(btx);
	}
	
	

	
	
}
