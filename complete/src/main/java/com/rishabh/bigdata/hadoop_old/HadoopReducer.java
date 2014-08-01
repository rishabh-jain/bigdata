package com.rishabh.bigdata.hadoop_old;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopReducer extends Reducer<IntWritable, Text, Text, IntWritable>{

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		int total_count = 0;
		
		for (Text value : values) {
			total_count++;
		}
		
		//super.reduce(arg0, arg1, arg2);
	}

}
