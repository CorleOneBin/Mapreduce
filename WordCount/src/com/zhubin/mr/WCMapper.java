package com.zhubin.mr;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

/**
 * ��ȡ��ʱ����һ��һ�ж����������ı��ļ���������keyinӦ�����±꣨��������valueinӦ����һ���ַ���
 * �����ʱ���ǵ��ʣ�������map
 * ����LongWritable, Text, Text, IntWritable
 * @author BIN
 *
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	/**
	 *key��value�Ƕ�����������
	 *ͨ��contextд��ȥ
	 */
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String str = value.toString();
		String[] strs = StringUtils.split(str, ' ');
		for(String s:strs) {
			context.write(new Text(s), new IntWritable(1));	//ͨ��context.write()����д��ȥ
			
		}
	}
}
