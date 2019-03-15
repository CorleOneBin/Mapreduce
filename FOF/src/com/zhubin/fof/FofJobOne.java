package com.zhubin.fof;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FofJobOne {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			conf.set("fs.defaultFS","hdfs://node1:8020");     //处于active的节点
			conf.set("yarn.resourcemanager.hostname","node3");  //处于active的节点
			Job job = Job.getInstance(conf);
			
			job.setJarByClass(FofJobOne.class);
			job.setMapperClass(FofMapperOne.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setReducerClass(FofReduceOne.class);
			FileInputFormat.addInputPath(job, new Path("/fof/input/friend"));
			Path outPath = new Path("/fof/output");
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(outPath)) {
				fs.delete(outPath);
			}
			FileOutputFormat.setOutputPath(job, outPath);
			
			boolean flag =  job.waitForCompletion(true);
			if(flag) {
				System.out.println("Job Success ! ! !");
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
