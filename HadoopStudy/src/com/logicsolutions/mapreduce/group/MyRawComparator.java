package com.logicsolutions.mapreduce.group;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

import com.logicsolutions.mapreduce.group.SortApp.NewK2;


/**
 * 业务要求按照第一列分组，但是NewK2的规则决定了不能按照第一列分组。只能自定义分组比较器
 * @author ThinkPad
 *
 */
public class MyRawComparator implements RawComparator<NewK2> {

	public int compare(NewK2 o1, NewK2 o2) {
		return (int)(o1.first - o2.first);
	}
	
	/* (non-Javadoc)
	 * arg0表示第一个参与比较的字节数组
	 * arg1表示第一个参与比较的字节数组的起始位置
	 * arg2表示第一个参与比较的字节数组的偏移量
	 * 
	 * arg3表示第二个参与比较的字节数组
	 * arg4表示第二个参与比较的字节数组的起始位置
	 * arg5表示第二个参与比较的字节数组的偏移量
	 * @see org.apache.hadoop.io.RawComparator#compare(byte[], int, int, byte[], int, int)
	 */
	public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
		return WritableComparator.compareBytes(arg0, arg1, 8, arg3, arg4, 8);
	}

	
	
}
