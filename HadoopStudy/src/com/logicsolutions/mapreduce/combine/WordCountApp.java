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
		
		//1.指定输入在哪里
		FileInputFormat.setInputPaths(job, WordCountApp.commaSeparatedPaths);
		//指定对输入数据进行格式化处理的类
		job.setInputFormatClass(TextInputFormat.class);
		
		
		//2.指定自定义的Mapper类
		job.setMapperClass(WordCountApp.MyMapper.class);
		//指定Mapper输出的key value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//3.分区
		job.setPartitionerClass(org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		//4. 排序  分组
		
		//5. 归约
		job.setCombinerClass(MyReduce.class); 
		
		
		//1. 指定定义的reduce函数
		job.setReducerClass(WordCountApp.MyReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
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
		ToolRunner.run(new WordCountApp(), args);
	}
	
	/**
	 *  KEYIN           K1  每一行的起始位置
 	 *  VALUEIN         V1  每一行的文本内容
	 *  KEYOUT          K2  每一行中的每个单词
	 *  VALUEOUT        V2  每一行中的每个单词的出现次数
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
	 * KEYIN           K2  每一行中的每个单词
	 * VALUEIN         V2  每一行中的每个单词的出现次数     固定值1
	 * KEYOUT          K3 每个文件中的单词
	 * VALUEOUT        V3  每一行中的每个单词的出现次数
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
