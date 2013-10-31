package com.funshion.hadoop.test.newImplements2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CountAllMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
	IntWritable outputValueIntWritable = new IntWritable();
	Text key = new Text();
	@Override
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {
		//1. count all word in each line
		//2. make a key
		String[] values = arg1.toString().split("\\W+");
		outputValueIntWritable.set(values.length);
		key.set("countAll");
		arg2.collect(key, outputValueIntWritable);
	}

}
