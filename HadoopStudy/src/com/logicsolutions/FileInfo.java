package com.logicsolutions;

import java.net.URI;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.FileStatus;  
import org.apache.hadoop.fs.Path;  
public class FileInfo{
  public static void main(String[] args) throws Exception{  
    if (args.length != 1){
      System.err.println("Usage: fileinfo <source>");
      System.exit(2);
    }
    Configuration conf = new Configuration();  
    FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
    FileStatus stat = fs.getFileStatus(new Path(args[0]));
    System.out.println(stat.getPath());  
    System.out.println(stat.getLen());  
    System.out.println(stat.getModificationTime());  
    System.out.println(stat.getOwner());  
    System.out.println(stat.getReplication());  
    System.out.println(stat.getBlockSize());  
    System.out.println(stat.getGroup());  
    System.out.println(stat.getPermission().toString());  
  }  
}