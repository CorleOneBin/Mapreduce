package com.zhubin.fof2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FofGroup extends WritableComparator{
	
	public FofGroup() {
		super(FOF.class,true);
	}

	public int compare(WritableComparable a, WritableComparable b) {
		FOF f1 = (FOF)a;
		FOF f2 = (FOF)b;
		int c1 = f1.getFriend1().compareTo(f2.getFriend1()); //根据用户名分组
		return c1;
	}
	
}
