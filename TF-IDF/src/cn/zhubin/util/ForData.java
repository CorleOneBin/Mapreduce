package cn.zhubin.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 * ��һƪ���£�10��һ���ַ������У�������id��ģ������
 * @author BIN
 *
 */
public class ForData {
	public static void main(String[] args) {
		try {
			File file = new File("H://456.txt");
			if(file.exists()) {
				file.delete();
				file.createNewFile();
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("H://123.txt")));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,true)));
			StringBuilder sb = new StringBuilder();
			String str = null;
			String line = null;
			int id = 30000000;
			int tmp;
			while((str=br.readLine()) != null) {
				str = str.replace(" ", "");    //ȥ���հ�
				str = str.replace("����", "");
				tmp=str.length()/30;           //30���ַ���Ϊһһ��
				for(int i = 0 ; i < tmp; i++) {   
					line=str.substring(i*30, (i+1)*30);
					bw.write(id+"\t"+line+"\r\n");
					id++;
				}
				if(str.length() > tmp*30+1) {
					line = str.substring(tmp*30);
					bw.write(id+"\t"+line+"\r\n");
					id++;
				}
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}







