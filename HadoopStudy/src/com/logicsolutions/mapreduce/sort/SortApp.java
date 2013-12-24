package com.logicsolutions.mapreduce.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class SortApp {
	
	static final String commaSeparatedPaths = "hdfs://192.168.42.118:9000/user/root/sort";
	private static final String OUT_PATH = "hdfs://192.168.42.118:9000/user/root/result";

	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(new URI(commaSeparatedPaths), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		Job job = new Job(conf, SortApp.class.getSimpleName());
		
		//job.setJarByClass(SortApp.class);
		
		//1.指定输入在哪里
		FileInputFormat.setInputPaths(job, SortApp.commaSeparatedPaths);
		//指定对输入数据进行格式化处理的类
		job.setInputFormatClass(TextInputFormat.class);
		
		//2.指定自定义的Mapper类
		job.setMapperClass(MyMapper.class);
		//指定Mapper输出的key value类型
		job.setMapOutputKeyClass(NewK2.class);
		job.setMapOutputValueClass(LongWritable.class);

		//3.分区
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		//4. 排序  分组
		
		//5. 归约
		
		//1. 指定定义的reduce函数
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		//2. 指定输出的路径
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));

		//3. 指定输出的格式化类
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//把作业提交给jobtracker执行
		job.waitForCompletion(true);
		
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, NewK2, LongWritable>{

		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, NewK2, LongWritable>.Context context)
				throws IOException, InterruptedException {
			final String[] splited = value.toString().split("\t");
			NewK2 newK2 = new NewK2(new LongWritable(Long.parseLong(splited[0])), new LongWritable(Long.parseLong(splited[1])));
			context.write(newK2, new LongWritable(Long.parseLong(splited[1])));
		}
		
		
	}
	
	static class MyReducer extends Reducer<NewK2, LongWritable, LongWritable, LongWritable>{

		protected void reduce(NewK2 k2, Iterable<LongWritable> v2s, org.apache.hadoop.mapreduce.Reducer<NewK2, LongWritable, LongWritable, LongWritable>.Context context) throws IOException, InterruptedException {
			for (LongWritable v2 : v2s) {
				context.write(new LongWritable(k2.first), v2);
			}
		}
		
	}
	
	static class NewK2 implements WritableComparable<NewK2>{
		
		Long first;
		Long second;
		
		
		
		public NewK2(LongWritable longWritable, LongWritable longWritable2) {
			this.first = longWritable.get();
			this.second = longWritable2.get();
		}
		
		public NewK2() {
			// TODO Auto-generated constructor stub
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.first = in.readLong();
			this.second = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(first);
			out.writeLong(second);
		}

		/* (non-Javadoc)
		 * 当K2进行排序时，会调用该方法
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		@Override
		public int compareTo(NewK2 o) {
			
			long minus = this.first - o.first;
			if(minus != 0){
				return (int)minus;
			}
			
			return (int)(this.second - o.second);
			
		}

		@Override
		public boolean equals(Object obj) {
			if(!(obj instanceof NewK2)){
				return false;
			}else{
				NewK2 newk2 = (NewK2)obj;
				return (this.first == newk2.second && this.second == newk2.second);
			}
		}

		@Override
		public int hashCode() {
			return this.first.hashCode() + this.second.hashCode();
		}
		
		
	}
}
