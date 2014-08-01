package com.rishabh.bigdata.hadoop_old;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.rishabh.bigdata.log.Logger;

public class HadoopMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {

		String mDataLine = value.toString();
		
		StringTokenizer mTokenizer = new StringTokenizer(mDataLine, ",");
		
		int token_number = 0;
		
		while (mTokenizer.hasMoreTokens()) {
			token_number++;
			context.write(new IntWritable(token_number), new Text(mTokenizer.nextToken()));
		}
		
		Logger.getInstance().logInfo("Hadoop Mapper", context.toString());
		//super.map(key, value, context);
	}

}
