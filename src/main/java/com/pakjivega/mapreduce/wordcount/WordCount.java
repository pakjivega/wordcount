package com.pakjivega.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

  public static void main(String[] args) throws Exception {

    int res = ToolRunner.run(new Configuration(), new WordCount(), args);
    System.exit(res);    
  }
  
  public int run(String[] args) throws Exception {
    //Create configuration
    Configuration conf = this.getConf();
    //Create job
    Job job = Job.getInstance(conf, "Tool word count");
    job.setJarByClass(WordCount.class);
    //Setup MapReduce
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);
    job.setCombinerClass(WordCountReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //Set the Input File
    FileInputFormat.addInputPath(job, new Path(args[0]));
    //Set the Output File
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }
}