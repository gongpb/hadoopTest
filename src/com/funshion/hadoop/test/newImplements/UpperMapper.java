package com.funshion.hadoop.test.newImplements;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class UpperMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
	IntWritable outputValueIntWritable = new IntWritable();
	Text key = new Text();
	@Override
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {
		//1. read all word
		//2. change
		String[] values = arg1.toString().split("\\W+");
		/**
		 * 含有大小H转换为小写h
		 */
		for(String v:values){
			if(v.contains("H")){
				v.replace("H", "h");
			}
		}
		
		outputValueIntWritable.set(values.length);
		key.set("countAll");
		arg2.collect(key, outputValueIntWritable);
	}
}