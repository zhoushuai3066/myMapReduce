package com.zhoushuai.myMapReduce.writableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MyPairWritable implements WritableComparable<MyPairWritable> {
	
	Text first;
	IntWritable second;
	
	public void set(Text first,IntWritable second){
		this.first = first;
		this.second = second;
	}
	
	public Text getFirst(){
		return first;
	}
	
	public IntWritable getSecond(){
		return second;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(first.toString());
		out.writeInt(second.get());
	}

	public void readFields(DataInput in) throws IOException {
		first = new Text(in.readUTF());
		second = new IntWritable(in.readInt());
	}

	public int compareTo(MyPairWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean equals(Object obj) {
		MyPairWritable temp = (MyPairWritable)obj;
		return first.equals(temp.getFirst()) && second.equals(temp.getSecond());
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 188 + second.hashCode();
	}

	@Override
	public String toString() {
		return first.toString() + " " + second.toString();
	}
	
	
	
	
	

}
