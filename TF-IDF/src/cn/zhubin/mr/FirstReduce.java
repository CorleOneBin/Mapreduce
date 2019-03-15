package cn.zhubin.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

	protected void reduce(Text key, Iterable<IntWritable> value,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable i : value) {
			sum = sum + i.get();
		}
		context.write(new Text(key), new IntWritable(sum));
	}
	
}
