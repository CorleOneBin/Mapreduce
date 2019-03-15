import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 矩阵相乘
 * @author BIN
 *
 */
public class Step4 {
	
	public static boolean run(Configuration config , Map<String,String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			
			job.setJobName("Step4");
			job.setJarByClass(Step4.class);
			job.setMapperClass(Step4_Mapper.class);
			job.setReducerClass(Step4_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(paths.get("Step4Input")));
			Path outPath = new Path(paths.get("Step4Output"));
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
	
	static class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text>{
		private String flag;
		
		protected void setup(Context context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit)context.getInputSplit();
			flag = split.getPath().getParent().getName();   //判断读取的数据是同现矩阵还是用户评分
		}
		/**
		 * 同现
		 * i100:i100   3
		 * i100:i106   1
		 * i100:i101   3
		 * i100:i102   1
		 * 用户评分
		 * u232   i100:2,i1323:4
		 */
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Pattern.compile("[\t,]").split(value.toString());  //用\t或,来切分
			
			if(flag.equals("Step3")) {     //是同现矩阵
				// 样本  i100:i100   3
				//    i100:i106   1
				String[] vs = tokens[0].split(":");
				String item1 = vs[0];
				String item2 = vs[1];
				String num = tokens[1];
				context.write(new Text(item1),new Text("A:"+item2 + "," + num));  //根据一个物品分组， 得到的就是那一行的数据
			}else if(flag.equals("Step2")) {  //用户评分矩阵
				/**
				 * 样本    u234	i100:2,i300:1
				 */
				String userID = tokens[0];
				for(int i = 1; i < tokens.length; i++) {
					String[] vector = tokens[i].split(":");
					String itemID = vector[0];
					String num = vector[1];
					context.write(new Text(itemID), new Text("B:"+userID+","+num)); //根据物品分组，得出的就是一个所有用户对这个物品的喜爱度，
				}
			}
		}
		
	}
	
	static class Step4_Reducer extends Reducer<Text, Text, Text, Text>{

		protected void reduce(Text key, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {
			//用来放一个物品（key）对应的其他所有物品的同现次数
			Map<String,Integer> mapa = new HashMap<>();
			
			//用来放一个物品(key) ， 对应的所有用户对其的推荐权重分数
			Map<String,Integer> mapb = new HashMap<>();
			
			for(Text line : iterable) {
				String value = line.toString();
				if(value.startsWith("A:")) {   //同现矩阵
					String[] kv = Pattern.compile("[\t,]").split(value.substring(2));
					mapa.put(kv[0], Integer.parseInt(kv[1]));
				}else if(value.startsWith("B:")) {
					String[] kv = Pattern.compile("[\t,]").split(value.substring(2));
					mapb.put(kv[0], Integer.parseInt(kv[1]));
				}
			}
			
			double result  = 0;
			Iterator<String> iter = mapa.keySet().iterator();
			while(iter.hasNext()) {
				String itemID = iter.next();
				int num = mapa.get(iter.next());
				Iterator<String> iterb = mapb.keySet().iterator();
				while(iterb.hasNext()) {
					String userID = iterb.next();
					double pref = mapb.get(userID);
					result = num * pref;
					context.write(new Text(userID), new Text(itemID + "," + result));
				}
			}
			//结果样本	u2343	i9,8.0
		}
		
	}
	
}
