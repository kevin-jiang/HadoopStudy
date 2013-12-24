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
		
		//1.指定输入在哪里
		FileInputFormat.setInputPaths(job, TopKApp.commaSeparatedPaths);
		//指定对输入数据进行格式化处理的类
		job.setInputFormatClass(TextInputFormat.class);
		
		
		//2.指定自定义的Mapper类
		job.setMapperClass(TopKApp.MyMapper.class);
		//指定Mapper输出的key value类型
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//3.分区
		job.setPartitionerClass(org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		//4. 排序  分组
		
		//5. 归约
		job.setCombinerClass(MyReduce.class); 
		
		
		//1. 指定定义的reduce函数
		job.setReducerClass(TopKApp.MyReduce.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		//2. 指定输出的路径
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));

		//3. 指定输出的格式化类
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		//把作业提交给jobtracker执行
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
	 *  KEYIN           K1  每一行的起始位置
 	 *  VALUEIN         V1  每一行的文本内容
	 *  KEYOUT          K2  每一行中的每个单词
	 *  VALUEOUT        V2  每一行中的每个单词的出现次数
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
	 * KEYIN           K2  每一行中的每个单词
	 * VALUEIN         V2  每一行中的每个单词的出现次数     固定值1
	 * KEYOUT          K3 每个文件中的单词
	 * VALUEOUT        V3  每一行中的每个单词的出现次数
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
