package cn.zhubin.mr;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class FirstPartition extends HashPartitioner<Text, IntWritable>{

	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		if(key.equals("count")) {
			return 1;
		}
		return super.getPartition(key, value, numReduceTasks-1);
	}
	
}
