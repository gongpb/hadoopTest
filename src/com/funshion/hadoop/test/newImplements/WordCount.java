package com.funshion.hadoop.test.newImplements;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		
		if(args.length!=2){
			System.err.println("Usage: WordCount <input path> <output path>");
			System.exit(-1);
		}
		
		JobConf conf = new JobConf(getConf(), WordCount.class);
		conf.setJobName("WordCount");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(CountAllMapper.class);
		conf.setReducerClass(CountAllReducer.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		
		return 0;
	}

	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new WordCount(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
