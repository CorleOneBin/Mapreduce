package com.zhubin.fof2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile.Sorter;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FofSort extends WritableComparator{
	
	public FofSort() {
		super(FOF.class,true);
	}

	/**
	 * 根据用户名升序，亲密度降序
	 */
	public int compare(WritableComparable a, WritableComparable b) {
		FOF f1 = (FOF)a;
		FOF f2 = (FOF)b;
		int c1 = f1.getFriend1().compareTo(f2.getFriend1());
		if(c1 == 0) {
				return -Integer.compare(f1.getNum(), f2.getNum());
		}
		return c1;
	}

}
