package com.logicsolutions.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import com.logicsolutions.mapreduce.KpiWritable;

public class KpiPartitioner extends HashPartitioner<Text, KpiWritable> {

	@Override
	public int getPartition(Text key, KpiWritable value, int numReduceTasks) {
		// TODO Auto-generated method stub
		//return super.getPartition(key, value, numReduceTasks);
		return key.toString().length() == 11 ? 0 : 1;
	}
	
}
