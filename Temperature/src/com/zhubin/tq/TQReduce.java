package com.zhubin.tq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TQReduce extends Reducer<Weather, IntWritable, Text, NullWritable>{

	@Override
	/**
	 * 对每一组来的任务进行处理， 以分好组，以年月分组，且以温度排好序。 所以只要取前两个最高温就可以了
	 */
	protected void reduce(Weather wea, Iterable<IntWritable> iterable,
			Context context) throws IOException, InterruptedException {
		int flag = 0;
		for(IntWritable i : iterable) {
			flag++;
			if(flag > 2) {
				break;
			}
			String msg = wea.getYear() + "-" + wea.getMonth() + "-" + wea.getDay() + "-" + i.get();
			context.write(new Text(msg), NullWritable.get());
		}
	}
}
