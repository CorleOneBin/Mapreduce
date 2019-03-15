package com.zhubin.fof2;

import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.StringUtils;

public class FOF implements WritableComparable<FOF>{

	private String friend1;
	private String friend2;
	private int num;
	
	public String getFriend1() {
		return friend1;
	}

	public void setFriend1(String friend1) {
		this.friend1 = friend1;
	}

	public String getFriend2() {
		return friend2;
	}

	public void setFriend2(String friend2) {
		this.friend2 = friend2;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	public void readFields(DataInput in) throws IOException {
		this.friend1 = in.readUTF();
		this.friend2 = in.readUTF();
		this.num = in.readInt();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(friend1);
		out.writeUTF(friend2);
		out.writeInt(num);
	}

	public int compareTo(FOF fof) {
		int c1 = this.getFriend1().compareTo(fof.getFriend1());
		if(c1 == 0) {
			
				return -Integer.compare(this.getNum(), fof.getNum());
			
		}
		return c1;
	}
	
}
