package com.logicsolutions.mapreduce.partition;

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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Reducer;


public class KpiApp {

	static final String commaSeparatedPaths = "hdfs://192.168.42.118:9000/user/root/wlan";
	private static final String OUT_PATH = "hdfs://192.168.42.118:9000/user/root/result.txt";

	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(new URI(commaSeparatedPaths), conf);
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		Job job = new Job(conf, KpiApp.class.getSimpleName());
		
		job.setJarByClass(KpiApp.class);
		//1.指定输入在哪里
		FileInputFormat.setInputPaths(job, KpiApp.commaSeparatedPaths);
		//指定对输入数据进行格式化处理的类
		job.setInputFormatClass(TextInputFormat.class);
		
		//2.指定自定义的Mapper类
		job.setMapperClass(KpiApp.MyMapper.class);
		//指定Mapper输出的key value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(KpiWritable.class);

		//3.分区
		job.setPartitionerClass(KpiPartitioner.class);
		job.setNumReduceTasks(2);
		
		//4. 排序  分组
		
		//5. 归约
		
		//1. 指定定义的reduce函数
		job.setReducerClass(KpiApp.MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KpiWritable.class);
		
		//2. 指定输出的路径
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));

		//3. 指定输出的格式化类
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//把作业提交给jobtracker执行
		job.waitForCompletion(true);
		
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, Text, KpiWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			final String[] splited = value.toString().split("\t");
			final String msisdn = splited[1];
			final Text k2 = new Text(msisdn);
			final KpiWritable kpiWritable = new KpiWritable(Long.parseLong(splited[6]),Long.parseLong(splited[7]),Long.parseLong(splited[8]),Long.parseLong(splited[9]));
			context.write(k2, kpiWritable);
		}
	}
	
	static class MyReducer extends Reducer<Text, KpiWritable, Text, KpiWritable>{

		/* 
		 * k2  表示整个文件中不同的手机号码
		 * v2s表示该手机号在不同行（也就是不同时段）中的流量的集合
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@SuppressWarnings("unchecked")
		protected void reduce(Text k2, Iterable<KpiWritable> v2s, @SuppressWarnings("rawtypes") org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
			long upPackNum = 0L;
			long downPackNum = 0L;
			long upPayLoad = 0L;
			long downPayLoad = 0L;
			for (KpiWritable kpiWritable : v2s) {
				upPackNum += kpiWritable.upPackNum;
				downPackNum += kpiWritable.downPackNum;
				upPayLoad += kpiWritable.upPayLoad;
				downPayLoad += kpiWritable.downPayLoad;
			}
			context.write(k2, new KpiWritable(upPackNum, downPackNum, upPayLoad, downPayLoad));
		}
		
	}
	
	static class KpiWritable implements Writable {
		
		public long upPackNum;
		public long downPackNum;
		public long upPayLoad;
		public long downPayLoad;
		
		
		
		public KpiWritable() {
			super();
		}

		public KpiWritable(long upPackNum, long downPackNum, long upPayLoad,
				long downPayLoad) {
			super();
			this.upPackNum = upPackNum;
			this.downPackNum = downPackNum;
			this.upPayLoad = upPayLoad;
			this.downPayLoad = downPayLoad;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.upPackNum = in.readLong();
			this.downPackNum = in.readLong();
			this.upPayLoad = in.readLong();
			this.downPayLoad = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(upPackNum);
			out.writeLong(downPackNum);
			out.writeLong(upPayLoad);
			out.writeLong(downPayLoad);
		}

		@Override
		public String toString() {
			return Long.toString(this.upPackNum) + "\t" + Long.toString(this.downPackNum) + "\t" 
					+ Long.toString(this.upPayLoad) + "\t" + Long.toString(this.downPayLoad);
		}
		
	}
	
	static class KpiPartitioner extends HashPartitioner<Text, KpiWritable> {

		@Override
		public int getPartition(Text key, KpiWritable value, int numReduceTasks) {
			// TODO Auto-generated method stub
			//return super.getPartition(key, value, numReduceTasks);
			return key.toString().length() == 11 ? 0 : 1;
		}
		
	}

}
