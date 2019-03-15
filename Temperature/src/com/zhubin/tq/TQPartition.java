package com.zhubin.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
/**
 * 重写Partition 分区
 * @author BIN
 *
 */
public class TQPartition extends HashPartitioner<Weather, IntWritable>{

	//规则 ： 1.满足业务  2.越简单越好
	public int getPartition(Weather key, IntWritable value, int numReduceTasks) {
		//return (key.getYear()-1949) % numReduceTasks;
		return super.getPartition(key, value, numReduceTasks);
	}
	
}
