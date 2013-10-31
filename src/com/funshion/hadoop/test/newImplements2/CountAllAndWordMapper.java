package com.funshion.hadoop.test.newImplements2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CountAllAndWordMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
	Text outputValueIntWritable = new Text();
	Text key = new Text();
	//arg0:行号
	@Override
	public void map(LongWritable arg0, Text arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3)
			throws IOException {
		//1. count all word in each line
		//2. make a key
		//3. continue count word
		System.out.println("----------mapper="+arg1.toString());
		String[] values = arg1.toString().split("\\W+");
		//计算每行的单词总数
		outputValueIntWritable.set(values.length+"");
		key.set("countAll");
		arg2.collect(key, outputValueIntWritable);
		//
		key.set("wordcount");
		for(String str:values){
			outputValueIntWritable.set(arg0+"!-!-!"+str);
			arg2.collect(key, outputValueIntWritable);
		}
	}
}
