package com.logicsolutions.mapreduce.combine;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

public class WordCountApp extends Configured implements Tool{
	
	static final String commaSeparatedPaths = "hdfs://kevinhadoop:9000/user/root/readme.txt";
	private static final String OUT_PATH = "hdfs://kevinhadoop:9000/user/root/result.txt";
	
	
	@Override
	public int run(String[] arg0) throws Exception {
		
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(new URI(commaSeparatedPaths), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		Job job = new Job(conf, WordCountApp.class.getSimpleName());
		job.setJarByClass(WordCountApp.class);
		
		//1.ָ������������
		FileInputFormat.setInputPaths(job, WordCountApp.commaSeparatedPaths);
		//ָ�����������ݽ��и�ʽ���������
		job.setInputFormatClass(TextInputFormat.class);
		
		
		//2.ָ���Զ����Mapper��
		job.setMapperClass(WordCountApp.MyMapper.class);
		//ָ��Mapper�����key value����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//3.����
		job.setPartitionerClass(org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		//4. ����  ����
		
		//5. ��Լ
		job.setCombinerClass(MyReduce.class); 
		
		
		//1. ָ�������reduce����
		job.setReducerClass(WordCountApp.MyReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
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
		ToolRunner.run(new WordCountApp(), args);
	}
	
	/**
	 *  KEYIN           K1  ÿһ�е���ʼλ��
 	 *  VALUEIN         V1  ÿһ�е��ı�����
	 *  KEYOUT          K2  ÿһ���е�ÿ������
	 *  VALUEOUT        V2  ÿһ���е�ÿ�����ʵĳ��ִ���
	 *
	 */
	static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		@SuppressWarnings("unchecked")
		@Override
		protected void map(LongWritable key, Text value, @SuppressWarnings("rawtypes") org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
			String[] splited = value.toString().split(" ");
			for (String word : splited) {
				context.write(new Text(word), new LongWritable(1L));
			}
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
	static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable>{
			
		@SuppressWarnings("unchecked")
		protected void reduce(Text k2, Iterable<LongWritable> v2s, @SuppressWarnings("rawtypes") org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
			long count = 0L;
			for (LongWritable v2 : v2s) {
				count += v2.get();
				System.out.println("The count of Text is: " + String.valueOf(count));
			}
			context.write(k2, new LongWritable(count));
		}
		
		
	}
}
