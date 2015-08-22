package com.pakjivega.mapreduce.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
public class WordCountMapperReducerTest {

	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	  MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	 
	  @Before
	  public void setUp() {
	    WordCountMapper mapper = new WordCountMapper();
	    WordCountReducer reducer = new WordCountReducer();
	    mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	  }
	 
	  @Test
	  public void testMapper() throws IOException {
	    mapDriver.withInput(new Text(), new Text(
	        "This This"));
	    mapDriver.withOutput(new Text("This"), new IntWritable(1));
	    mapDriver.withOutput(new Text("This"), new IntWritable(1));
	    mapDriver.runTest();
	  }
	 
	  @Test
	  public void testReducer() throws IOException{
	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    reduceDriver.withInput(new Text("This"), values);
	    reduceDriver.withOutput(new Text("This"), new IntWritable(2));
	    reduceDriver.runTest();
	  }
	   
	  @Test
	  public void testMapReduce() throws IOException{
	    mapReduceDriver.withInput(new Text(), new Text("This This "));
	    //List<IntWritable> values = new ArrayList<IntWritable>();
	    //values.add(new IntWritable(1));
	    //values.add(new IntWritable(1));
	    mapReduceDriver.withOutput(new Text("This"), new IntWritable(2));
	    //mapReduceDriver.withOutput(new Text("is"), new IntWritable(3));
	    //mapReduceDriver.withOutput(new Text("a"), new IntWritable(1));
	    //mapReduceDriver.withOutput(new Text("file"), new IntWritable(1));	    
	    mapReduceDriver.runTest();
	  }
}
