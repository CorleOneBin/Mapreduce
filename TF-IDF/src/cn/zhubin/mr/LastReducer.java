package cn.zhubin.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LastReducer extends Reducer<Text, Text, Text, Text>{

	protected void reduce(Text arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		for(Text text : arg1) {
			sb.append(text.toString() + "\t");
		}
		arg2.write(arg0,new Text(sb.toString()));  //最后输出结果:   300012	早餐：3.12		豆浆：3.12  。。。。。。
	}
	
}
