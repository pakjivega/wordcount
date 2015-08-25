package com.pakjivega.mapreduce.wordcouples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCouplesMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		String previous = itr.nextToken();
		while (itr.hasMoreTokens()) {
			word.set( previous + " " + itr.nextToken());
			context.write(word, one);
		}
	}
}