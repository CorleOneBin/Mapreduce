import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;


/**
 * 构建用户评分矩阵
 * @author BIN
 *
 */
public class Step2 {
	
	public static boolean run(Configuration config , Map<String,String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			job.setJobName("Step2");
			job.setJarByClass(Step2.class);
			job.setMapperClass(Step2_Mapper.class);
			job.setReducerClass(Step2_Reducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(paths.get("Step2Input")));
			Path outPath = new Path(paths.get("Step2Output"));
			if(fs.exists(outPath)) {
				fs.delete(outPath);
			}
			
			boolean flag = job.waitForCompletion(true);
			return flag;
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	  return false;
	}
	
	static class Step2_Mapper extends Mapper<LongWritable, Text, Text, Text>{

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = StringUtils.split(value.toString(),',');
			String item = tokens[0];
			String user = tokens[1];
			String action = tokens[2];
			int rv = StartRun.R.get(action);
			context.write(new Text(user), new Text(item+":"+rv));
		}
		
	}
	
	static class Step2_Reducer extends Reducer<Text, Text, Text, Text>{

		protected void reduce(Text key, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {
			Map<String,Integer> r = new HashMap<>();
			
			for (Text value : iterable) {
				String[] vs = StringUtils.split(value.toString(), ':');
				String item = vs[0];
				Integer action = Integer.parseInt(vs[1]);
				action = ((Integer)(r.get(item) == null ? 0 : r.get(item))).intValue() + action;
				r.put(item, action);
			}
			StringBuilder sb = new StringBuilder();
			for(Map.Entry<String,Integer> entry : r.entrySet()) {
				sb.append(entry.getKey() + ":" + entry.getValue() + ",");
			}
			context.write(key, new Text(sb.toString()));
		}
		
	}
}
