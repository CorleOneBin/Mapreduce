package com.zhubin.tq;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TQJob {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			conf.set("fs.defaultFS","hdfs://node1:8020");     //处于active的节点
			conf.set("yarn.resourcemanager.hostname","node3");  //处于active的节点
			Job job = Job.getInstance(conf);
			//conf.set("mapred.jar","D://mac.jar"); 
			
			job.setJarByClass(TQJob.class);
			job.setMapperClass(TQMapper.class);
			job.setMapOutputKeyClass(Weather.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setPartitionerClass(TQPartition.class);
			job.setSortComparatorClass(TQSort.class);
			job.setGroupingComparatorClass(TQGroup.class);
			//job.setNumReduceTasks(3);
			
			job.setReducerClass(TQReduce.class);
			FileInputFormat.addInputPath(job, new Path("/weather/input/tq"));
			Path outPath = new Path("/weather/output");
			FileSystem fs = FileSystem.get(conf);
			
			if(fs.exists(outPath)) {
				fs.delete(outPath);
			}
			
			FileOutputFormat.setOutputPath(job, outPath);
			boolean flag =  job.waitForCompletion(true);
			if(flag) {
				System.out.println("Job success !!!");
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
