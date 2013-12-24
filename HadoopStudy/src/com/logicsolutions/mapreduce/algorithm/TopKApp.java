package com.logicsolutions.mapreduce.algorithm;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopKApp extends Configured implements Tool{
	
	static final String commaSeparatedPaths = "hdfs://192.168.42.118:9000//user/root/user/root/topk";
	private static final String OUT_PATH = "hdfs://192.168.42.118:9000/user/root/result";
	
	
	@Override
	public int run(String[] arg0) throws Exception {
		
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(new URI(commaSeparatedPaths), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		Job job = new Job(conf, TopKApp.class.getSimpleName());
		job.setJarByClass(TopKApp.class);
		
		//1.ָ������������
		FileInputFormat.setInputPaths(job, TopKApp.commaSeparatedPaths);
		//ָ�����������ݽ��и�ʽ���������
		job.setInputFormatClass(TextInputFormat.class);
		
		
		//2.ָ���Զ����Mapper��
		job.setMapperClass(TopKApp.MyMapper.class);
		//ָ��Mapper�����key value����
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//3.����
		job.setPartitionerClass(org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		//4. ����  ����
		
		//5. ��Լ
		job.setCombinerClass(MyReduce.class); 
		
		
		//1. ָ�������reduce����
		job.setReducerClass(TopKApp.MyReduce.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		//2. ָ�������·��
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));

		//3. ָ������ĸ�ʽ����
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		//����ҵ�ύ��jobtrackerִ��
		job.waitForCompletion(true);
		
		return 0;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TopKApp(), args);
	}
	
	/**
	 *  KEYIN           K1  ÿһ�е���ʼλ��
 	 *  VALUEIN         V1  ÿһ�е��ı�����
	 *  KEYOUT          K2  ÿһ���е�ÿ������
	 *  VALUEOUT        V2  ÿһ���е�ÿ�����ʵĳ��ִ���
	 *
	 */
	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
		Long max = Long.MIN_VALUE;
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context) throws IOException, InterruptedException {
			Long temp = Long.parseLong(value.toString());
			if( temp > max ){
				max = temp;
			}
		}
		
		protected void cleanup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context) throws IOException, InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		}
		
		
	}
	
	/**
	 * KEYIN           K2  ÿһ���е�ÿ������
	 * VALUEIN         V2  ÿһ���е�ÿ�����ʵĳ��ִ���     �̶�ֵ1
	 * KEYOUT          K3 ÿ���ļ��еĵ���
	 * VALUEOUT        V3  ÿһ���е�ÿ�����ʵĳ��ִ���
	 * @author ThinkPad
	 *
	 */
	static class MyReduce extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable>{
		
		Long max = Long.MIN_VALUE;
		protected void reduce(LongWritable k2, Iterable<NullWritable> v2s, org.apache.hadoop.mapreduce.Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context context) throws IOException, InterruptedException {
			Long temp = k2.get();
			if( temp > max ){
				max = temp;
			}
			
			
		}
		
		protected void cleanup(org.apache.hadoop.mapreduce.Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context context) throws IOException,
				InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		}
		
		
	}
}
