package com.zhubin.fof2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FofJobTwo {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			conf.set("fs.defaultFS","hdfs://node1:8020");     //处于active的节点
			conf.set("yarn.resourcemanager.hostname","node4");  //处于active的节点
			Job job = Job.getInstance(conf);
			
			job.setJarByClass(FofJobTwo.class);
			job.setMapperClass(FofMapperTwo.class);
			job.setMapOutputKeyClass(FOF.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setGroupingComparatorClass(FofGroup.class);
			job.setSortComparatorClass(FofSort.class);
			job.setReducerClass(FofReduceTwo.class);
			FileInputFormat.addInputPath(job, new Path("/fof/input/friend2"));
			Path outPath = new Path("/fof/output");
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(outPath)) {
				fs.delete(outPath);
			}
			FileOutputFormat.setOutputPath(job, outPath);
			boolean flag = job.waitForCompletion(true);
			if(flag) {
				System.out.println("Job success");
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
