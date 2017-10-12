package my;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//by Xiaoting
public class twoe_nocombiner 
{
//One mapper to read AccessLog dataset
	 public static class accessmap 
	    extends Mapper<LongWritable, Text, LongWritable, Text>{
		  private LongWritable personID = new LongWritable();
		  private Text pageInfo = new Text();
		  //private String fileTag = "M";
	   
		  public void map(LongWritable key, Text value, Context context
	                 ) throws IOException, InterruptedException {
			  String line = value.toString();//change each line to string
			  //System.err.println("line:"+line);
			  String[] splits = line.split(",");//seperate line
			 
			  
			  personID.set(Long.parseLong(splits[1]));//get ByWho
			  pageInfo.set(splits[2]);//get WhatPage

			  /*System.err.println("map");
			  System.err.println(personID);
			  System.err.println(pageInfo);*/
			  context.write(personID, pageInfo);
	 }
	 }
	 //get key:ByWho-value:WhatPage pairs 
	 public static class accessReducer 
	 extends Reducer<LongWritable, Text,LongWritable,Text> {
		  
		  String name;
		 
		  public void reduce(LongWritable personID, Iterable<Text> values , 
	                 Context context) throws IOException, InterruptedException {
			  int number=0;//number of distinct Facebook pages
			  int number1=0;//number of total Facebook pages
			  //System.err.println("uuoueioopw");
			  //System.err.println(personID);
			  //System.err.println(values);
			  Set <String> pages=new HashSet<String>();//create a hashset store each WhatPage tuple
			  for (Text val : values) {
				  number1+=1;
				  //System.err.println(number1);
				  if(pages.add(val.toString().split(",")[0])){
					  number+=1;//if not stored in the hashset
					  //System.err.println(number);
					 
					  
				  }
				  
				 
			  }
				  
			  Text results = new Text();
			  String num=Integer.toString(number)+","+Integer.toString(number1);
			  System.err.println(num);
			  results.set(num);
			  
			  context.write(personID,results);
		}

			  
	 }
	 public static void main(String[] args) throws Exception{
			// TODO Auto-generated method stub
			Configuration conf = new Configuration();
		   if (args.length != 2) {
		     System.err.println("Usage: querye <HDFS input file1> <HDFS input file2> <HDFS output file>");
		     System.exit(3);
		   }
		   Job job = new Job(conf, "query2e");
		   job.setJarByClass(twoe_nocombiner.class);
		   job.setMapperClass(accessmap.class);//set mapper
		   //job.setCombinerClass(accessReducer.class);
		   job.setReducerClass(accessReducer.class);//set reducer
		   job.setNumReduceTasks(1);

		   job.setOutputKeyClass(LongWritable.class);//specify output format
		   job.setOutputValueClass(Text.class);
		   FileInputFormat.addInputPath(job, new Path(args[0]));
		   FileOutputFormat.setOutputPath(job, new Path(args[1]));
		   System.exit(job.waitForCompletion(true) ? 0 : 1);
			}

		}
		 
	 

	 