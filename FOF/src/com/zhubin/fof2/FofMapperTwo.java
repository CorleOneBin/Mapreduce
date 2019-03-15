package com.zhubin.fof2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class FofMapperTwo extends Mapper<LongWritable, Text, FOF, IntWritable>{

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] strs = StringUtils.split(value.toString(), '-');
		FOF fof1 = new FOF();
		fof1.setFriend1(strs[0]);
		fof1.setFriend2(strs[1]);
		int num = Integer.parseInt(strs[2]);
		fof1.setNum(num);
		context.write(fof1,new IntWritable(num));
		
		FOF fof2 = new FOF();
		fof2.setFriend1(strs[1]);
		fof2.setFriend2(strs[0]);
		int num1 = Integer.parseInt(strs[2]);
		context.write(fof2, new IntWritable(num1));
		
		}
}
