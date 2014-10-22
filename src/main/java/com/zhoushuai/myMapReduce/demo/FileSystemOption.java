package com.zhoushuai.myMapReduce.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class FileSystemOption {
	
	public static void main(String[] args){
		Configuration conf = new Configuration();
		System.out.println(conf);
		try {
			System.out.println(System.getenv("YARN_HOME"));
			FileSystem fs = FileSystem.get(conf);
			Path dir = new Path("/user/zhoushuai");
			Path file = new Path("/user/zhoushuai/input.txt");
			fs.mkdirs(dir);
			fs.copyFromLocalFile(new Path("C:\\Users\\zhoushuai\\Desktop\\mapreduce\\input.txt"), file);
			FileStatus status = fs.getFileStatus(file);
			System.out.println(status.getPath());
			System.out.println(status.getPath().toUri().getPath());
			System.out.println(status.getBlockSize());
			System.out.println(status.getGroup());
			System.out.println(status.getOwner());
//			fs.delete(file,true);
			System.out.println(fs.isFile(file));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
