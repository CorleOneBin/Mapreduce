import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * 构建同现矩阵
 * i100:i100   3
 * i100:i106   1
 * i100:i101   3
 * i100:i102   1
 * i100:i103   3
 * i100:i104   1
 * i100:i105   3
 * i100:i107   1
 * @author BIN
 *
 *是根据用户对物品的评分矩阵来的， 两两组合
 *
 */
public class Step3 {
	
	public static boolean run(Configuration config , Map<String,String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance();
			job.setJobName("Step3");
			job.setJarByClass(Step3.class);
			job.setMapperClass(Step3_Mapper.class);
			job.setReducerClass(Step3_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(paths.get("Step3Inputtf")));
			Path outPath = new Path(paths.get("Step3Output"));
			if(fs.exists(outPath)) {
				fs.delete(outPath);
			}
			FileOutputFormat.setOutputPath(job, outPath);
			
			boolean flag = job.waitForCompletion(true);
			
			return flag;
			
		} catch (IllegalStateException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	  return false;
	}
	
	static class Step3_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		//样本  u2847	i531:1,i543:4,i242:6       这代表一个用户同时对这三个物品有操作，两两组合后，代表有一个人同时对这两个物品有操作
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = StringUtils.split(value.toString(),'\t');
			String[] items = StringUtils.split(tokens[1],',');
			for(int i = 0 ; i < items.length; i++) {
				String itemA = StringUtils.split(items[i],':')[0];
				for(int j = 0; j < items.length; j++) {
					String itemB = StringUtils.split(items[i],':')[0];
					context.write(new Text(itemA + ":" + itemB), new IntWritable(1));
				}
			}
		}
	}
	
	static class Step3_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		protected void reduce(Text text, Iterable<IntWritable> iterable,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable i:iterable) {
				sum+=i.get();
			}
			context.write(text, new IntWritable(sum));
		}
		
	}
	
}
