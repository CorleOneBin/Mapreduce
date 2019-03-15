package cn.zhubin.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

/**
 * 求出某个词语在几条语句中出现了
 * @author BIN
 *
 */
public class TwoJob {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		Job job = null;
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			job = Job.getInstance(conf);
			job.setJarByClass(TwoJob.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setMapperClass(TwoMapper.class);
			job.setReducerClass(TwoReduce.class);
			
			FileInputFormat.addInputPath(job, new Path(""));
			Path outPath = new Path("");
			if(fs.exists(outPath)) {
				fs.delete(outPath);
			}
			FileOutputFormat.setOutputPath(job, outPath);
			
			boolean flag = job.waitForCompletion(true);
			if(flag) {
				System.out.println("success");
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
