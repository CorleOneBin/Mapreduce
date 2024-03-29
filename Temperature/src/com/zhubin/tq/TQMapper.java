package com.zhubin.tq;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;




/**
 * 从文本文件读数据，一行一行读数据。所以key-value是 行数，字符
 * 出去的时候key是一个weather对象，而value是温度
 * @author BIN
 *
 */
public class TQMapper extends Mapper<LongWritable, Text, Weather, IntWritable>{
	
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String[] strs = StringUtils.split(value.toString(),'\t');
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		try {
			
			cal.setTime(sdf.parse(strs[0]));
			Weather weather = new Weather();
			weather.setYear(cal.get(Calendar.YEAR));
			weather.setMonth(cal.get(Calendar.MONTH)+1);
			weather.setDay(cal.get(Calendar.DAY_OF_MONTH));
			int wd = Integer.parseInt(strs[1].substring(0, strs[1].lastIndexOf("c")));
			weather.setWd(wd);
			context.write(weather, new IntWritable(wd));
		
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}