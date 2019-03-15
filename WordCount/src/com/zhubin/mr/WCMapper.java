package com.zhubin.mr;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

/**
 * 读取的时候，是一行一行读（超大型文本文件），所以keyin应该是下标（行数），valuein应该是一行字符串
 * 输出的时候，是单词，个数的map
 * 所以LongWritable, Text, Text, IntWritable
 * @author BIN
 *
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	/**
	 *key，value是读进来的数据
	 *通过context写出去
	 */
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String str = value.toString();
		String[] strs = StringUtils.split(str, ' ');
		for(String s:strs) {
			context.write(new Text(s), new IntWritable(1));	//通过context.write()方法写出去
			
		}
	}
}
