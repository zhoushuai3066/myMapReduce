package com.zhoushuai.myMapReduce.writableComparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.zhoushuai.myMapReduce.writableComparable.MyPairWritable;



public class ValueSortComparator extends WritableComparator {
	
	public ValueSortComparator(){
		super(MyPairWritable.class,true);
	}


	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MyPairWritable p1 = (MyPairWritable) a;
		MyPairWritable p2 = (MyPairWritable) b;
		if(!p1.getFirst().toString().equals(p2.getFirst().toString())){
			return p1.getFirst().toString().compareTo(p2.getFirst().toString());
		}else{
			return p1.getSecond().get() - p2.getSecond().get();
		}
	}
	
	

	
	
}
