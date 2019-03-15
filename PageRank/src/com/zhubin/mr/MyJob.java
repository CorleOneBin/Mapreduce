package com.zhubin.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyJob {
	
	public static enum Mycounter{
		//枚举
		my
	}
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://node1:8020");     //处于active的节点
		conf.set("yarn.resourcemanager.hostname","node4");  //处于active的节点
		double d = 0.001;
		int i = 0;
		//一直循环到满足收敛要求
		while(true) {
			i++;
			try {
				conf.setInt("runCount", i);
				Job job = Job.getInstance(conf);
				FileSystem fs = FileSystem.get(conf);	
				
				job.setJarByClass(MyJob.class);
				job.setJobName("pr"+i);
				job.setMapperClass(MyMapper.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setInputFormatClass(KeyValueTextInputFormat.class);  //设置文本文件为Text：Text读入，以‘/t’分割   而不是以long，text读入
				
				job.setReducerClass(MyReducer.class);
				
				Path inputPath = new Path("/pagerank/input/pr");
				if(i > 1) {
					inputPath = new Path("/pagerank/output/pr" + (i-1));
				}
				
				FileInputFormat.addInputPath(job, inputPath);
				
				Path outPath = new Path("/pagerank/output/pr"+i);
				if(fs.exists(outPath)) {
					fs.delete(outPath);
				}
				
				FileOutputFormat.setOutputPath(job, outPath);
				boolean f = job.waitForCompletion(true);
				if(f) {
					long sum = job.getCounters().findCounter(Mycounter.my).getValue();
					double avgd = sum /4000.0;
					if(avgd < d) {
						break;
					}
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
	
	static class MyMapper extends Mapper<Text, Text, Text, Text>{
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			int runCount = context.getConfiguration().getInt("runCount", 1);
			Node node = null;
			if(runCount == 1) {
				node = Node.fromMR("1.0" + "\t" + value.toString());  //初始的时候设票数为1
			}else {
				node = Node.fromMR(value.toString());   //以上次投票的为基准
			}
			
			//输出的值  A:1.0 B D
			context.write(new Text(key.toString()), new Text(node.toString()));
			
			if(node.containAdjacentNodes()) {
				double outValue = node.getPageRank()/node.getAdjacentNodeNames().length;
				for(int i = 0; i < node.getAdjacentNodeNames().length; i++) {
					String outPage = node.getAdjacentNodeNames()[i];
					//计算被投节点的值  输出： B:0.5  D:0.5
					context.write(new Text(outPage), new Text(outValue+""));
				}
			}
			
		}
		
	}
	
	static class MyReducer extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {
			double sum = 0.0 ;
			Node sourceNode = null;
			for (Text text1 : iterable) {
				Node node = Node.fromMR(text1.toString());
				if(node.containAdjacentNodes()) {
					sourceNode = node;
				}
				else {
					sum += node.getPageRank();
				}
			}
			
			//计算新的PR值，并和旧的比较
			double newPR = (0.15 /4.0) + (0.85 * sum);
			double d = sourceNode.getPageRank() - newPR;
			int j = (int)(d * 1000);
			j = Math.abs(j); 
			context.getCounter(Mycounter.my).increment(j);
			sourceNode.setPageRank(newPR);   //换成新的pr值，输出去，用作下一次的比较
			context.write(new Text(key.toString()), new Text(sourceNode.toString()));
		}
	}
	
	
}
