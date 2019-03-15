package com.zhubin.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * 重写sort，按照时间（年月）升序 ， 如果时间相同，则按照温度降序
 * @author BIN
 *
 */
public class TQSort extends WritableComparator{
	
	//必须写空的构造方法   且必须加参数ture
	public TQSort() {
		super(Weather.class,true);
	}

	public int compare(WritableComparable a, WritableComparable b) {
		Weather w1 = (Weather)a;
		Weather w2 = (Weather)b;
		int c1 = Integer.compare(w1.getYear(), w2.getYear());
			if(c1 == 0) {
				int c2 = Integer.compare(w1.getMonth(), w2.getMonth());
				if(c2 == 0) {
					return -Integer.compare(w1.getWd(), w2.getWd());
				}
				return c2;
			}
			return c1;
	}
}
