package com.logicsolutions.mapreduce.oldapi;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;



/**
 * 
 * hadoop版本1.x的包一般是mapreduce
 * hadoop版本0.x的包一般是mapred
 * @author ThinkPad
 *
 */
public class OldApp {

	
	static final String commaSeparatedPaths = "hdfs://kevinhadoop:9000/user/root/readme.txt";
	private static final String OUT_PATH = "hdfs://kevinhadoop:9000/user/root/result.txt";

	/**
	 * 不在使用job对象  ，而是使用jobconf
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(new URI(commaSeparatedPaths), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		JobConf job = new JobConf(conf, OldApp.class);
		
		//1.指定输入在哪里
		String commaSeparatedPaths;
		FileInputFormat.setInputPaths(job, OldApp.commaSeparatedPaths);
		//指定对输入数据进行格式化处理的类
		job.setInputFormat(TextInputFormat.class);
		
		//2.指定自定义的Mapper类
		job.setMapperClass(OldApp.MyMapper.class);
		//指定Mapper输出的key value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//3.分区
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		//4. 排序  分组
		
		//5. 归约
		
		
		//1. 指定定义的reduce函数
		job.setReducerClass(OldApp.MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//2. 指定输出的路径
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));

		//3. 指定输出的格式化类
		job.setOutputFormat(TextOutputFormat.class);
		
		
		//把作业提交给jobtracker执行
		JobClient.runJob(job);
	}
	
	
	static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>{
		@Override
		public void map(LongWritable k1, Text v1, OutputCollector<Text, LongWritable> collector, Reporter repoter) throws IOException {
			
			//计数器
			Counter counter = repoter.getCounter("Sensitive Counter", "Hello Counter");
			
			String value = v1.toString();
			if(value.contains("Hello")){
				counter.increment(1L);
			}
			
			
			String[] splited = value.split(" ");
			for (String word : splited) {
				collector.collect(new Text(word), new LongWritable(1L));
			}
		}
		
	}
	
	static class MyReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>{

		@Override
		public void reduce(Text k2, Iterator<LongWritable> v2s, OutputCollector<Text, LongWritable> collector, Reporter repoter) throws IOException {
			long count = 0L;
			while (v2s.hasNext()) {
				count += v2s.next().get();
				System.out.println("The count of Text is: " + String.valueOf(count));
			}
			collector.collect(k2, new LongWritable(count));
		}
		
		
	}

}
