package com.zhubin.fof2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FofReduceTwo extends Reducer<FOF, IntWritable, Text, NullWritable>{

	protected void reduce(FOF fof, Iterable<IntWritable> iterable,
			Context context) throws IOException, InterruptedException {
		for(IntWritable i : iterable) {
			String msg = fof.getFriend1()+" -> "+fof.getFriend2()+"   "+""+i.get();
			context.write(new Text(msg), NullWritable.get());
		}
	}
}
