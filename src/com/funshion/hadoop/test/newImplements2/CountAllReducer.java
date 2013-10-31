package com.funshion.hadoop.test.newImplements2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CountAllReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
	Text outputIntWritable = new Text();
	Text key = new Text();
	@Override
	public void reduce(Text arg0, Iterator<Text> arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3)
			throws IOException {
		
		if(arg0.toString().equals("countall")){
			int sum =0;
			while (arg1.hasNext()) {
				arg2.collect(key, outputIntWritable);
				String valueStrings = arg1.next().toString();
				sum += Integer.parseInt(valueStrings);
			}
			key.set("result");
			outputIntWritable.set(sum+"");
			arg2.collect(key, outputIntWritable);
		} else {
			int totalWord = 0;
			Map<String, String> word = new HashMap<String, String>();
			Text value = null;
			String valueString = null;
			String[] values1 = null;
			while (arg1.hasNext()) {
				value = arg1.next();
				valueString = value.toString();
				System.out.println("----------valueString="+valueString);
				values1 = valueString.split("!-!-!");
				word.put(values1[1], values1[1]);
			}
			totalWord = word.keySet().size();
			
			key.set("wordcont");
			outputIntWritable.set(totalWord+"");
			arg2.collect(key, outputIntWritable);
		}
		
	}

}
