package cn.zhubin.mr;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] v = value.toString().split("\t");
		if(v.length >= 2) {
			String id = v[0].trim();
			String content = v[1].trim();
			
			StringReader sr = new StringReader(content);
			IKSegmenter iksegmenter = new IKSegmenter(sr, true);
			Lexeme word = null;
			while((word = iksegmenter.next()) != null) {
				String w = word.getLexemeText();
				context.write(new Text(w+"_"+id), new IntWritable(1));
			}
			context.write(new Text("count"), new IntWritable(1));
		}
	}
}
