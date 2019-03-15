package com.zhubin.fof;

public class Util {
	public String format(String str1, String str2) {
		int c1 = str1.compareTo(str2);
		if(c1 < 0) {
			return str1 + "-" + str2;
		}
		return str2 + "-" + str1;
	}
}
