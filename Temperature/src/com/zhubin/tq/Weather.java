package com.zhubin.tq;

import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * 天气对象
 * 要进行序列化和反序列化的操作  ， 并且要进行排序
 * @author BIN
 *
 */
public class Weather implements WritableComparable<Weather>{
	
	private int year;
	private int month;
	private int day;
	private int wd;   //温度
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getWd() {
		return wd;
	}

	public void setWd(int wd) {
		this.wd = wd;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.day = in.readInt();
		this.wd = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(wd);
	}

	/**
	 * 会用到的情况：1.两个weather对象进行比较
	 * 		   2.排序和分组     如果没有重写方法，就会来用这个Comparable方法
	 */
	public int compareTo(Weather o) {
		int c1 = Integer.compare(this.year, o.year);
			if(c1 == 0) {
				int c2 = Integer.compare(this.month, o.month);
				if(c2 == 0) {
					int c3 = Integer.compare(this.wd, o.wd);
					return c3;
				}
				return c2;
			}
		return c1;
	}

}
