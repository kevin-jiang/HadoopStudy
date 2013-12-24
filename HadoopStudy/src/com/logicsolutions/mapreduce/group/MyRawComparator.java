package com.logicsolutions.mapreduce.group;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

import com.logicsolutions.mapreduce.group.SortApp.NewK2;


/**
 * ҵ��Ҫ���յ�һ�з��飬����NewK2�Ĺ�������˲��ܰ��յ�һ�з��顣ֻ���Զ������Ƚ���
 * @author ThinkPad
 *
 */
public class MyRawComparator implements RawComparator<NewK2> {

	public int compare(NewK2 o1, NewK2 o2) {
		return (int)(o1.first - o2.first);
	}
	
	/* (non-Javadoc)
	 * arg0��ʾ��һ������Ƚϵ��ֽ�����
	 * arg1��ʾ��һ������Ƚϵ��ֽ��������ʼλ��
	 * arg2��ʾ��һ������Ƚϵ��ֽ������ƫ����
	 * 
	 * arg3��ʾ�ڶ�������Ƚϵ��ֽ�����
	 * arg4��ʾ�ڶ�������Ƚϵ��ֽ��������ʼλ��
	 * arg5��ʾ�ڶ�������Ƚϵ��ֽ������ƫ����
	 * @see org.apache.hadoop.io.RawComparator#compare(byte[], int, int, byte[], int, int)
	 */
	public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
		return WritableComparator.compareBytes(arg0, arg1, 8, arg3, arg4, 8);
	}

	
	
}
