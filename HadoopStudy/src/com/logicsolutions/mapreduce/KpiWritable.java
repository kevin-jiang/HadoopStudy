package com.logicsolutions.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class KpiWritable implements Writable {
	
	long upPackNum;
	long downPackNum;
	long upPayLoad;
	long downPayLoad;
	
	
	
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
