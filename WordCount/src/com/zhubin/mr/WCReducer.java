package com.zhubin.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * keyin��valuesin��map�������һ���ģ���Ϊreducer�����ݾ��Ǵ�map�ù�����
 * keyout��valuesout���Լ������ʲô
 * @author BIN
 *
 */
public class WCReducer extends Reducer<Text , IntWritable, Text, IntWritable>{
	/**
	 * ͨ��������������
	 * �õ��������Ѿ��ֺ��� ������������������<hello,{1,1,1}> (û��conbiner���̵������)
	 */
	protected void reduce(Text text, Iterable<IntWritable> arg1,Context arg2) 
			throws IOException, InterruptedException {
		
		int sum = 0;
		for(IntWritable i:arg1) {
			sum+=i.get();
		}
		
		arg2.write(text, new IntWritable(sum));
	}
}
