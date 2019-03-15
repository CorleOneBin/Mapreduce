package cn.zhubin.mr;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 第一个MR
 * 算出TF（一个单词在其句子中出现的次数）
 * 算出总的句子个数（用另一个reduce输出，与数据分开）
 * @author BIN
 *
 */
public class JobOne {
	
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS","hdfs://node1:8020");     //处于active的节点
			conf.set("yarn.resourcemanager.hostname","node3");  //处于active的节点
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			job.setJarByClass(JobOne.class);
			job.setMapperClass(FirstMapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setNumReduceTasks(2);
			job.setCombinerClass(FirstReduce.class);
			job.setPartitionerClass(FirstPartition.class);
			job.setReducerClass(FirstReduce.class);
			
			FileInputFormat.addInputPath(job, new Path("/tfidf/input"));
			Path outPath = new Path("/tfidf/output/output1");
			if(fs.exists(outPath)) {
				fs.delete(outPath);
			}
			FileOutputFormat.setOutputPath(job, outPath);
			job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
}
