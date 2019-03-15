package com.zhubin.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * keyin和valuesin和map的输出是一样的，因为reducer的数据就是从map拿过来的
 * keyout和valuesout看自己想输出什么
 * @author BIN
 *
 */
public class WCReducer extends Reducer<Text , IntWritable, Text, IntWritable>{
	/**
	 * 通过迭代器拿数据
	 * 拿到的数据已经分好组 即过来的数据类似于<hello,{1,1,1}> (没有conbiner过程的情况下)
	 */
	protected void reduce(Text text, Iterable<IntWritable> arg1,Context arg2) 
			throws IOException, InterruptedException {
		
		int sum = 0;
		for(IntWritable i:arg1) {
			sum+=i.get();
		}
		
		arg2.write(text, new IntWritable(sum));
	}
}
