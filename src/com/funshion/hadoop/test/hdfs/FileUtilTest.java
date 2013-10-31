package com.funshion.hadoop.test.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class FileUtilTest {
	private static Configuration conf = null;
	static {
		conf = new Configuration();
	}

	public static void copy(){
		FileSystem fs = null;
		FileUtil fileUtil = new FileUtil();
		
		try {
			fs = FileSystem.get(conf);
			File file = new File("E:\\md5sum.txt");
			//copy to hdfs
//			fileUtil.copy(file, fs, new Path("/user/root/test.txt"), false, conf);
			//copy to local
			fileUtil.copy(fs, new Path("/user/root/test.txt"), file, true, conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		copy();
	}
}
