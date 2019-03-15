package com.zhubin.mr;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

public class Node {
	//��ʼPRֵΪ1
	private double pageRank = 1.0;
	
	//�ַ����� ���汻ͶƱ�Ľڵ�
	private String[] adjacentNodeNames;
	
	private static final char filedSeparator = '\t';

	public double getPageRank() {
		return pageRank;
	}

	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}

	public String[] getAdjacentNodeNames() {
		return adjacentNodeNames;
	}

	public void setAdjacentNodeNames(String[] adjacentNodeNames) {
		this.adjacentNodeNames = adjacentNodeNames;
	}
	
	//�ж��Ƿ��нڵ�
	public boolean containAdjacentNodes() {
		return adjacentNodeNames != null && adjacentNodeNames.length > 0;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(pageRank);
		if(getAdjacentNodeNames() != null) {
			sb.append(filedSeparator).append(StringUtils.join(getAdjacentNodeNames(),filedSeparator));
		}
		return sb.toString();
	}
	
	public static Node fromMR(String values) {
		String[] parts = StringUtils.split(values,filedSeparator );
		Node node = new Node();
		node.setPageRank(Double.parseDouble(parts[0]));
		if(parts.length > 1) {
			node.setAdjacentNodeNames(Arrays.copyOfRange(parts, 1, parts.length));
		}
		return node;
	}
	
}
