package com.zhoushuai.myMapReduce.job;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import com.zhoushuai.myMapReduce.map.ValueSortMapper;
import com.zhoushuai.myMapReduce.writableComparable.MyPairWritable;
import com.zhoushuai.myMapReduce.writableComparator.ValueSortComparator;




/**
 * Ҫ�Խ�����������Ҫ�Զ����Լ���������͡�Ȼ���Զ����Լ���comparator��������job���Զ������ͺ��Զ���comparator
 * 
aaa 12
eee 21
ccc 32
bbb 45
aaa 21
eee 54
ccc 59
bbb 32
aaa 54
eee 78
ccc 91
bbb 23
aaa 45
eee 78
ccc 45
bbb 21
�������
aaa 12
aaa 21
aaa 45
aaa 54
bbb 21
bbb 23
bbb 32
bbb 45
ccc 32
ccc 45
ccc 59
ccc 91
eee 21
eee 54
eee 78
eee 78




map������ǣ�
aaa 12
aaa 21
aaa 45
aaa 54
bbb 32
bbb 23
bbb 21
bbb 45
ccc 59
ccc 45
ccc 91
ccc 32
eee 78
eee 54
eee 78
eee 21
��ʱ��map�����ֻ�ǰ���key����������
��Ҫ��ͨ��
job.setSortComparatorClass();��map�Ľ����ж�������

 * @author zhoushuai
 *
 */
public class ValueSortJob {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Path path = new Path("/tmp/bbb.txt");
		Path outFile = new Path("/tmp/zhoushuaiccc");
		Job job = new Job();
		job.setJarByClass(ValueSortJob.class);
		
		FileInputFormat.addInputPath(job, path);
		FileOutputFormat.setOutputPath(job, outFile);
		
		job.setMapperClass(ValueSortMapper.class);
//		job.setReducerClass(MyReducer.class);
		
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(MyPairWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setSortComparatorClass(ValueSortComparator.class);
		
		job.waitForCompletion(true);
	}


}
