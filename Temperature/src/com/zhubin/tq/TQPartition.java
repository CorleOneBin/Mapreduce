package com.zhubin.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
/**
 * ��дPartition ����
 * @author BIN
 *
 */
public class TQPartition extends HashPartitioner<Weather, IntWritable>{

	//���� �� 1.����ҵ��  2.Խ��Խ��
	public int getPartition(Weather key, IntWritable value, int numReduceTasks) {
		//return (key.getYear()-1949) % numReduceTasks;
		return super.getPartition(key, value, numReduceTasks);
	}
	
}
