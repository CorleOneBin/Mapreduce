import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class StartRun {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS","hdfs://node1:8020");     //����active�Ľڵ�
		config.set("yarn.resourcemanager.hostname","node3");  //����active�Ľڵ�
		
		//����mr����������Ŀ¼������map������
		Map<String,String> paths = new HashMap<>();
		paths.put("Step1Input", "/user/itemcf/input/test");
		paths.put("Step1Output", "/user/itemcf/output/step1");
		paths.put("Step2Input", paths.get("Step1Output"));
		paths.put("Step2Output", "/user/itemcf/output/step2");
		
		Step1.run(config, paths);   //ȥ��
		
		
	}
	
	//������Ʒ�Ĳ�����ת��Ϊ�û��Դ���Ʒ�������
	public static Map<String,Integer> R = new HashMap<>();
	static {
		R.put("click", 1);
		R.put("collect", 2);
		R.put("cart", 3);
		R.put("alipay", 4);
	}
}
