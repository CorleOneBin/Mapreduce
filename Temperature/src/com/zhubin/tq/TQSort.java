package com.zhubin.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * ��дsort������ʱ�䣨���£����� �� ���ʱ����ͬ�������¶Ƚ���
 * @author BIN
 *
 */
public class TQSort extends WritableComparator{
	
	//����д�յĹ��췽��   �ұ���Ӳ���ture
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
