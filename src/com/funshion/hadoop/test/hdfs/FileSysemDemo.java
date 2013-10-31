package com.funshion.hadoop.test.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class FileSysemDemo {
	private static Configuration conf = null;
	static {
		conf = new Configuration();
	}
	/**
	 * 创建文件
	 * @param path
	 * @param content
	 */
	public void createFile(String path, String content){
		FileSystem fs = null;
		Path path2 = new Path(path);
		FSDataOutputStream outputStream = null;
		try {
			fs = FileSystem.get(conf);
			outputStream = fs.create(path2);
			outputStream.writeBytes(content);
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			if(outputStream==null){
				try {
					outputStream.close();
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	/**
	 * 删除文件、文件夹
	 * @param path
	 */
	public void deleteFile(String path){
		FileSystem fs = null;
		Path path2 = new Path(path);
		boolean result = false;
		try {
			fs = FileSystem.get(conf);
			result = fs.delete(path2, true);
			if(result){
				System.out.println("delete file "+path+" ok");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}
	/**
	 * 新建文件夹
	 * 1、get FileSystem instance
	 * 2、call method mkdirs
	 * @param path
	 */
	public void createFolder(String path){
		FileSystem fs = null;
		Path path2 = new Path(path);
		boolean result = false;
		try {
			fs = FileSystem.get(conf);
			result = fs.mkdirs(path2);
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}
	
	/**
	 * 追加文件内容
	 * 1、get FileSystem instance
	 * 2、call method append
	 * 3、should add property
	 * 4、FSDataOutputStream
	 * 5、use write method of FSDataOutputStream
	 * @param path
	 */
	public void appendFile(String path, String content){
		FileSystem fs = null;
		Path path2 = new Path(path);
		FSDataOutputStream outputStream = null;
		
		try {
			fs = FileSystem.get(conf);
			outputStream = fs.append(path2);
			outputStream.writeChars(content);
			outputStream.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			if(outputStream==null){
				try {
					outputStream.close();
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	/**
	 * 读取文件
	 * 1、create fs instance
	 * 2、call fs.open
	 * 3、FSDataInputStream read file
	 * @param path
	 */
	public  void readFile(String filePath){
		
		FileSystem fs = null;
		Path path = new Path(filePath); 
		FSDataInputStream inputStream = null;
		
		try {
			fs = FileSystem.get(conf);
			inputStream = fs.open(path);
			Text line = new Text();
			LineReader liReader = new LineReader(inputStream);
			while(liReader.readLine(line) > 0){
				System.out.println(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			try {
				inputStream.close();
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 修改文件名称
	 * @param oldFile
	 * @param newFile
	 */
	public void renameFile(String oldFile, String newFile){
		FileSystem fs = null;
		Path oldPath = new Path(oldFile);
		Path newPath = new Path(newFile);
		FSDataInputStream inputStream = null;
		boolean result = false;
		try {
			fs = FileSystem.get(conf);
			result = fs.rename(oldPath, newPath);
			if(result){
				System.out.println("rename file ok");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	/**
	 * 修改文件夹名称
	 * @param oldFile
	 * @param newFile
	 */
	public void renameFolder(String oldFile, String newFile){
		FileSystem fs = null;
		Path oldPath = new Path(oldFile);
		Path newPath = new Path(newFile);
		FSDataInputStream inputStream = null;
		boolean result = false;
		try {
			fs = FileSystem.get(conf);
			result = fs.rename(oldPath, newPath);
			if(result){
				System.out.println("rename folder ok");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 复制
	 * @param src
	 * @param hdfs
	 */
	public void copyFromLocal(String src, String hdfs){
		FileSystem fs = null;
		Path localPath = new Path(src);
		Path hdfsPath = new Path(hdfs);
		
		try {
			fs = FileSystem.get(conf);
//			fs.copyFromLocalFile(localPath, hdfsPath);
			//true：删除本地文件 ；默认是false，不删除本地文件
			fs.copyFromLocalFile(true,localPath, hdfsPath);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * 复制到local
	 * @param src
	 * @param hdfs
	 */
	public void copyToLocal(String src, String dist){
		FileSystem fs = null;
		Path srcPath = new Path(src);
		Path distPath = new Path(dist);
		
		try {
			fs = FileSystem.get(conf);
			//true：删除本地文件 ；默认是false，不删除本地文件
			fs.copyToLocalFile(true,srcPath, distPath);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	/**
	 * 移动
	 * @param src
	 * @param dist
	 */
	public void moveFromLocal(String src, String dist){
		FileSystem fs = null;
		Path srcPath = new Path(src);
		Path distPath = new Path(dist);
		try {
			fs = FileSystem.get(conf);
			//true：删除本地文件 ；默认是false，不删除本地文件
			fs.moveFromLocalFile(srcPath, distPath);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 移动
	 * @param src
	 * @param dist
	 */
	public void moveToLocal(String src, String dist){
		FileSystem fs = null;
		Path srcPath = new Path(src);
		Path distPath = new Path(dist);
		try {
			fs = FileSystem.get(conf);
			//true：删除本地文件 ；默认是false，不删除本地文件
			fs.moveToLocalFile(srcPath, distPath);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 获取系统属性
	 */
	public void fileSystemStatus(){
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			System.out.println("default block size "+ fs.getDefaultBlockSize() );
			System.out.println("uri："+fs.getUri());
			System.out.println("Relication："+fs.getDefaultReplication());
			System.out.println("Dictory："+fs.getHomeDirectory());
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void getFileStatus(String file){
		Path folder = new Path(file);
		System.out.println(folder.depth());
		System.out.println(folder.getName());
		System.out.println(folder.getParent());
	}
	
	public void getFileList(String path){
		FileSystem fs = null;
		Path folder = new Path(path);
		try {
			fs = FileSystem.get(conf);
			FileStatus [] list = fs.listStatus(folder);
			for(FileStatus fileStatus : list ){
				System.out.println(fileStatus.getReplication());
				System.out.println(fileStatus.getPath());
				System.out.println(fileStatus.getOwner());
				System.out.println(fileStatus.getBlockSize());
				System.out.println(fileStatus.getLen());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public static void main(String[] args) {
		FileSysemDemo createFile = new FileSysemDemo();
//		createFile.createFile("/user/root/firstHadoop2.txt","hadoop word");
//		createFile.createFolder("/user/root/first");
//		createFile.deleteFile("/user/root/first");
//		createFile.appendFile("/user/root/firstHadoop2.txt"," welcome to here ");
//		createFile.readFile("/user/root/firstHadoop2.txt");
//		createFile.renameFile("/user/root/firstHadoop2.txt","/user/root/firstHadoop.txt");
//		createFile.renameFolder("/user/root/test","/user/root/test1");
//		createFile.copyFromLocal("E:\\md5sum.txt", "/user/root/test3.txt");
//		createFile.copyToLocal("/user/root/test3.txt","E:\\md5sum.txt");
//		createFile.moveFromLocal("E:\\md5sum.txt", "/user/root/test4.txt");
//		createFile.moveToLocal("/user/root/test4.txt","E:\\md5sum.txt");
		
//		createFile.fileSystemStatus();
//		createFile.getFileList("/user/root/in");
		createFile.getFileStatus("/user/root");
	}
	
	
}
