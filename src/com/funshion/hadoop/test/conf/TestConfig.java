package com.funshion.hadoop.test.conf;

import org.apache.hadoop.conf.Configuration;

public class TestConfig {
	private static Configuration conf = null;
	static {
		conf = new Configuration();
		conf.addResource("myconf.xml");
	}
	public static void main(String[] args){
		System.out.println(conf.get("fs.default.name"));
		System.out.println(conf.get("myself"));
		
	}
}
