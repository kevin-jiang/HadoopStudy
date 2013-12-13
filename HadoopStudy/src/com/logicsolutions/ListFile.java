package com.logicsolutions;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.FileStatus;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.fs.FileUtil;  


public class ListFile{  
	
  public static void main(String[] args) throws Exception{
    if (args.length != 1){
      System.err.println("Usage: filelist <source>");
      System.exit(2);
    }
    Configuration conf = new Configuration();  
    FileSystem fs = FileSystem.get(URI.create(args[0]),conf);    
    FileStatus[] status = fs.listStatus(new Path(args[0]));
    Path[] listedPaths = FileUtil.stat2Paths(status);
    for(Path p : listedPaths){
      System.out.println(p);  
    }
  }  
}
