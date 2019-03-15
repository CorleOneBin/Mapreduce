import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class StartRun {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS","hdfs://node1:8020");     //处于active的节点
		config.set("yarn.resourcemanager.hostname","node3");  //处于active的节点
		
		//所有mr的输入和输出目录定义在map集合中
		Map<String,String> paths = new HashMap<>();
		paths.put("Step1Input", "/user/itemcf/input/test");
		paths.put("Step1Output", "/user/itemcf/output/step1");
		paths.put("Step2Input", paths.get("Step1Output"));
		paths.put("Step2Output", "/user/itemcf/output/step2");
		
		Step1.run(config, paths);   //去重
		
		
	}
	
	//将对商品的操作，转换为用户对此商品的在意度
	public static Map<String,Integer> R = new HashMap<>();
	static {
		R.put("click", 1);
		R.put("collect", 2);
		R.put("cart", 3);
		R.put("alipay", 4);
	}
}
