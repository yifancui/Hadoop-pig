package my;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
public class twof_nocombiner {
//mapper to read Friends dataset line by line
	 public static class friendmap 
	    extends Mapper<LongWritable, Text, LongWritable, Text>{
		  private LongWritable personID = new LongWritable();
		  private Text friendInfo = new Text();
		  
	   
		  public void map(LongWritable key, Text value, Context context
	                 ) throws IOException, InterruptedException {
			  //System.err.println("map");
			  String line = value.toString();
			  String[] splits = line.split(",");
			  int perID = Integer.parseInt(splits[1]);//get person id
			  String name = splits[2];//get MyFried(friends' ids)
			  personID.set(perID);
			  friendInfo.set("F,"+name);
			  context.write(personID, friendInfo);
	 }
	 }
	 //mapper to read AccessLog dataset line by line
	 public static class accessmap
	   extends Mapper<LongWritable, Text, LongWritable, Text>{
		  private LongWritable personID = new LongWritable();
		  private Text accessInfo= new Text();
		  
		  public void map(LongWritable key, Text value, Context context
	                ) throws IOException, InterruptedException {
			  //System.err.println("mapf");
			  String line = value.toString();
			  String[] splits = line.split(",");
			  personID.set(Long.parseLong(splits[1]));//get ByWho(person id)
			  String name = splits[2];//get WhatPage(accessed page ids)
			  
			  accessInfo.set("A,"+name);//put file flag
			  context.write(personID, accessInfo);
			  
	}
	}
	 
	 /*public static class accessmap
	   extends Mapper<LongWritable, Text, LongWritable, Text>{
		  private LongWritable personID = new LongWritable();
		  private Text friendInfo= new Text();
		  
		  
		  public void map(LongWritable key, Text value, Context context
	                ) throws IOException, InterruptedException {
			  System.err.println("mapa");
			  //access id
			  String accessid = value.toString().split(",")[2];
			  String line = value.toString();
			  String[] splits = line.split(",");
			  personID.set(Long.parseLong(splits[2]));
			  
			  friendInfo.set("A,1");
			  context.write(personID, friendInfo);
			  
	}
	}*/
	 //join Friends and AccessLog based on Person id
	 public static class joinReducer 
	 extends Reducer<LongWritable, Text,LongWritable,Text> {
		  String name;
		  
		  public void reduce(LongWritable personID, Iterable<Text> values , 
	                 Context context
	                 ) throws IOException, InterruptedException {
			  
			  Set <String> friends=new HashSet<String>();//create a hashset to store all the friends' ids
			  Set <String> access=new HashSet<String>();//create a hashset to store all the accessed page id
			  
			  for (Text val : values) {
				  if (val.toString().split(",")[0].equals("F")){
					  
					  
					  name = val.toString().split(",")[1];
					  friends.add(name);//store all the friends' ids based on file flag
					  //System.err.println(name);
				  }
				  else if (val.toString().split(",")[0].equals("A")){
					  String num = val.toString().split(",")[1];
					  access.add(num);//store all accessed page id based on file flag
					  //System.err.println(name);
					  
					  
					  
				  }
				  
			  
			  
		}	boolean diff=false;
			  Text results = new Text();
			  for(String friend:friends){
				  if(access.add(friend)){
					   diff=true;//if a friend never appears in the accessed page id hashset, we can conclude this Facebook owner has declared someone as their friend yet who never accessed their friends' page 
					  //
					  
				  }
			  }
			  if(diff){
				  results.set("this person has a friends never visited");
				  context.write(personID,results);
			  }
			  /*if(friends.removeAll(access)){
				  results.set("this person has a friends never visited");
				  
			  }
			  //else{ results.set("this person does not have a friends never visited");}
				  */
			  
			  
			  
			  //System.err.println(access);
			 // System.err.println(friends);

			  
	 }
	 }
		  

	 
		  public static void main(String[] args) throws Exception{
				// TODO Auto-generated method stub
				Configuration conf = new Configuration();
			    if (args.length != 3) {
			      System.err.println("Usage: queryf <HDFS input file1> <HDFS input file2> <HDFS output file>");
			      System.exit(3);
			    }
			    Job job = new Job(conf, "queryf");
			    job.setJarByClass(twof_nocombiner.class);
			    Path mypageInputPath = new Path(args[0]); 
			    Path friendInputPath = new Path(args[1]); 
			    Path outputPath = new Path(args[2]);

			    job.setReducerClass(joinReducer.class);//set reducer

			    job.setOutputKeyClass(LongWritable.class);
			    job.setOutputValueClass(Text.class);
			    job.setInputFormatClass(TextInputFormat.class);

			    MultipleInputs.addInputPath(job, mypageInputPath,//link one of the mappers to the file
			            TextInputFormat.class, friendmap.class);
			    MultipleInputs.addInputPath(job, friendInputPath,
			            TextInputFormat.class, accessmap.class);
			    FileOutputFormat.setOutputPath(job, outputPath);
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
				}

			}