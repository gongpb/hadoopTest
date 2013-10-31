package com.funshion.hadoop.test.mapReduce;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Tool 打印所有信息的属性
 * 1.extends  configured and implements Tool interface
 * @author gongpb
 */
public class TestTool extends Configured implements Tool{
	private static Configuration conf = null;
	static {
		conf = new Configuration();
	}
	@Override
	public int run(String[] args) throws Exception {
		for(Entry<String, String> entry:conf){
			System.out.println(entry.getKey()+" - "+entry.getValue());
		}
		return 0;
	}
    
	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new TestTool(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
