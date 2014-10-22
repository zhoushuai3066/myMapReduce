package com.zhoushuai.myMapReduce.partitioner;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * partitioner�ý��job.setNumReduceTasks(2);һ��ʹ��
 * @author zhoushuai
 *
 */
public  class TxtPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numpartitions) {
		if(key.toString().startsWith("a")||key.toString().startsWith("b")){
			return 0;
		}else{
			return 1;
		}
	}

}
