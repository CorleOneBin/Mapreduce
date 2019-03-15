package com.zhubin.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * 重写group ， 同年同月的分为一个组
 * @author BIN
 *
 */
public class TQGroup extends WritableComparator{
	
	public TQGroup() {
		super(Weather.class,true);
	}

	public int compare(WritableComparable a, WritableComparable b) {
		Weather w1 = (Weather)a;
		Weather w2 = (Weather)b;
		int c1 = Integer.compare(w1.getYear(), w2.getYear());
		   if(c1 == 0) {
			   return   Integer.compare(w1.getMonth(), w2.getMonth());
			    
		   }
			return c1;
	}
	
	
}
