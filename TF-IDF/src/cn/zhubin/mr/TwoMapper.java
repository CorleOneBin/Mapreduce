package cn.zhubin.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * value : ����_30000123 2
 * @author BIN
 *
 */
public class TwoMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		//��ȡ��ǰmapper task������Ƭ��  ��ֹ��count xxx���Ǹ��ļ�
		FileSplit fs = (FileSplit) context.getInputSplit();
		if(!fs.getPath().getName().contains("part-r-000001")) {
			String[] v = value.toString().trim().split("\t");
			if(v.length > 2) {
				String[] ss = v[0].split("_");
				if(ss.length > 2) {
					String w =ss[0];
					context.write(new Text(w), new IntWritable(1));  //����w����ʣ���ss[1]���idָ���ľ����г��ֹ�
				}
			}
		}
	}
	
	
	
}
