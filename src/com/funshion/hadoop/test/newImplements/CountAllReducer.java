package com.funshion.hadoop.test.newImplements;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CountAllReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
	IntWritable outputIntWritable = new IntWritable();
	Text key = new Text();
	@Override
	public void reduce(Text arg0, Iterator<IntWritable> arg1,
			OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {
		System.out.println("reduce: arg0="+arg0+" arg1="+arg1+" arg2="+arg2 +" arg3="+arg3);
		int sum =0;
		while (arg1.hasNext()) {
			sum += arg1.next().get();
		}
		key.set("result");
		outputIntWritable.set(sum);
		arg2.collect(key, outputIntWritable);
	}
}
