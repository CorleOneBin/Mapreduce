import java.io.IOException;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * ШЅжи
 * 
 * @author BIN
 *
 */
public class Step1 {
	public static boolean run(Configuration config , Map<String,String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			job.setJobName("Step1");
			job.setJarByClass(Step1.class);
			job.setMapperClass(Step1_Mapper.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.setReducerClass(Step1_Reducer.class);
			
			FileInputFormat.addInputPath(job, new Path(paths.get("Step1Input")));
			Path outPath = new Path(paths.get("Step1Output"));
			if(fs.exists(outPath)) {
				fs.delete(outPath,true);
			}
			FileOutputFormat.setOutputPath(job, outPath);
			
			boolean f = job.waitForCompletion(true);
			return f;
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	static class Step1_Mapper extends Mapper<LongWritable, Text, Text, NullWritable>{

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			if(key.get() != 0) {
				context.write(new Text(value), NullWritable.get());
			}
			
		}
	}
	
	static class Step1_Reducer extends Reducer<Text, NullWritable, Text, NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<NullWritable> value,
				Context context) throws IOException, InterruptedException {
			context.write(new Text(key), NullWritable.get());
		}
		
	}
}