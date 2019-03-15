package cn.zhubin.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;
/**
 *����tftdf  
 *�����н��ļ�������ӵ��ڴ��еĲ���������ֻ�����ύ���������ķ�ʽ������
 * @author BIN
 *
 */
public class LastJob {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			conf.set("maperd.jar", "D:\\MR\\weibo3.jar");
			Job job = Job.getInstance(conf);
			FileSystem fs  = FileSystem.get(conf);
			job.setJarByClass(LastJob.class);
			//��΢��������ӵ��ڴ���
			job.addCacheFile(new Path("/tfidf/output/weibo1/part-r-00001").toUri());
			
			//��df���ڶ���mr��������ݣ���ӵ��ڴ���
			job.addCacheFile(new Path("/tfidf/output/weibo2/part-r-00000").toUri());
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setMapperClass(LastMapper.class);
			job.setReducerClass(LastReducer.class);
			
			FileInputFormat.addInputPath(job, new Path("/tfidf/output/weibo1"));  //��һ��mr�����ֵ��Ϊ����
			Path outPath = new Path("/tfidf/output/weibo3");
			if(fs.exists(outPath)) {
				fs.delete(outPath);
			}
			FileOutputFormat.setOutputPath(job, outPath);
			
			boolean flag =  job.waitForCompletion(true);
			if(flag) {
				System.out.println("success");
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
