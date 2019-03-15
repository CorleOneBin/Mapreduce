package com.zhubin.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class FofMapperOne extends Mapper<LongWritable, Text, Text, IntWritable>{

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] strs = StringUtils.split(value.toString(), ' ');
		Util util = new Util();  //防止有 hadoop-tom   tom-hoadoop 出现
		
		for(int i = 0; i < strs.length; i++) {      //两两组合
			for(int j = i+1; j < strs.length; j++) {
				if(i == 0) {         //是真正的好友关系
					context.write(new Text(util.format(strs[i], strs[j])), new IntWritable(0));
				}else {
					context.write(new Text(util.format(strs[i], strs[j])), new IntWritable(1));
				}
			}
		}
	}
	
}
