import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 将矩阵相乘的值相加，得到最终的结果矩阵
 * @author BIN
 *
 */
public class Step5 {
	
	public static boolean run(Configuration config , Map<String,String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			
			job.setJobName("Step5");
			job.setJarByClass(Step5.class);
			job.setMapperClass(Step5_Mapper.class);
			job.setReducerClass(Step5_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(paths.get("Step5Input")));
			Path outPath = new Path(paths.get("Step5Output"));
			if(fs.exists(outPath)) {
				fs.delete(outPath);
			}
			FileOutputFormat.setOutputPath(job, outPath);
			
			return job.waitForCompletion(true);
			
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
		
	}
	
	static class Step5_Mapper extends Mapper<LongWritable,Text , Text, Text>{

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//样本	u2343	i9,8.0   直接输出
			String[] tokens = StringUtils.split(value.toString(),'\t');
			context.write(new Text(tokens[0]), new Text(tokens[1]));
		}
	}
	
	static class Step5_Reducer extends Reducer<Text, Text, Text, Text>{

		protected void reduce(Text key, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {
			Map<String,Double> map = new HashMap<>();
			
			for(Text line : iterable) {
				String[] tokens = line.toString().split(",");
				String item = tokens[0];
				double pref = Double.parseDouble(tokens[1]);
				if(map.containsKey(item)) {
					map.put(item, map.get(item)+pref);
				}else {
					map.put(item, pref);
				}
			}
			
			Iterator<String> iter = map.keySet().iterator();
			while(iter.hasNext()) {
				String item = iter.next();
				double score = map.get(item);
				context.write(key, new Text(item + "," + score));  
			}
			//输出的即为对应的推荐矩阵 中的一个值   :   u12	  i9,5.0
		}
		
	}
	
}
