package com.funshion.hadoop.test;


public class Test {

//	public static class TokenizeMapper extends Mapper{
//		
//		private final static IntWritable one = new IntWritable();
//		private Text word;
//		public void map( Object key, Text value, Context context ) throws IOException, InterruptedException{
//			StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
//			
//			while(stringTokenizer.hasMoreTokens()){
//				word.set(stringTokenizer.nextToken());
//				context.write(word, one);
//			}
//		}
//	}
	
	public static void main(String[] args) {
		System.out.println("the first hadoop test");
	}
}
