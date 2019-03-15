package com.zhubin.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FofReduceOne extends Reducer<Text, IntWritable, Text, NullWritable>{

	protected void reduce(Text text, Iterable<IntWritable> iterable,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		boolean flag = true;
		for (IntWritable it : iterable) {
			if(it.get() == 0) {
				flag = false;
				break;
			}
			sum++;
		}
		if(flag) {
			context.write(new Text(text.toString()+"-"+sum), NullWritable.get());
		}
		
	}
}
