package com.zhubin.mr;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCJob {
	 public static void main(String[] args) throws Exception {
		 
		Configuration conf = new Configuration();	//默锟斤拷锟斤拷src目录锟铰碉拷锟斤拷锟斤拷锟侥硷拷
		
		//conf.set("fs.defaultFS","hdfs://node1:8020");       //锟斤拷锟斤拷src目录锟铰碉拷锟斤拷锟斤拷锟侥硷拷锟斤拷锟皆硷拷锟斤拷锟矫ｏ拷锟斤拷win锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷
		//conf.set("yarn.resourcemanager.hostname","node3");  

		//conf.set("mapred.jar", "H:\\javaee\\jar\\wc.jar");
		  
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(WCJob.class); //指锟斤拷锟斤拷锟斤拷锟斤拷锟�
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);			//String
		job.setMapOutputValueClass(IntWritable.class);  //int
		
		job.setReducerClass(WCReducer.class);
		
		FileInputFormat.addInputPath(job, new Path("/wc/input"));
		Path outPath = new Path("/wc/output");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)) {						//锟斤拷止锟侥硷拷锟窖达拷锟斤拷
			fs.delete(outPath,true);
		}
		FileOutputFormat.setOutputPath(job, outPath);
		
		boolean flag = job.waitForCompletion(true);		//锟结交job
		
		if(flag) {
			System.out.println("Job success");
		}
		
	}
}
