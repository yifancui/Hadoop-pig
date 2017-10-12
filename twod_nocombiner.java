package my;


import java.io.IOException;
import org.apache.hadoop.mapreduce.counters.*;
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
//we have two mapper to deal with two different files
public class twod_nocombiner {
//We join MyPage dataset and Friends dataset on person id
	 public static class mypagemap //mapper class to read MyPage file line by line
	    extends Mapper<LongWritable, Text, LongWritable, Text>{
		  private LongWritable personID = new LongWritable();
		  private Text mypageInfo = new Text();
		  
	   
		  public void map(LongWritable key, Text value, Context context
	                 ) throws IOException, InterruptedException {
			  //System.err.println("map");
			  String line = value.toString();//change line to string
			  String[] splits = line.split(",");//split each string by comma
			  int perID = Integer.parseInt(splits[0]);//take person id from MyPage 
			  String name = splits[1];//take name 
			  personID.set(perID);
			  mypageInfo.set("M,"+name);//set file flag in the value
			  context.write(personID, mypageInfo);//write key(person id)-value(person name) pairs
	 }
	 }
	 public static class friendmap
	   extends Mapper<LongWritable, Text, LongWritable, Text>{//mapper class to read Friends file line by line
		  private LongWritable personID = new LongWritable();
		  private Text friendInfo= new Text();
		  
		  public void map(LongWritable key, Text value, Context context
	                ) throws IOException, InterruptedException {
			  //System.err.println("mapf");
			  String line = value.toString();//change line to string
			  String[] splits = line.split(",");//split each string by comma
			  personID.set(Long.parseLong(splits[2]));//take person id 
			  
			  friendInfo.set("F,1");//file flag for Friends file
			  context.write(personID, friendInfo);
			  
	}
	}
	 //reducer take key-value pairs from these two mappers, join MyPage and Friends on person id
	 public static class joinReducer 
	 extends Reducer<LongWritable, Text,LongWritable,Text> {
		  String name;
		  int number;//the number of people listing him or her as friend
		  public void reduce(LongWritable personID, Iterable<Text> values , 
	                 Context context
	                 ) throws IOException, InterruptedException {
			  number=0;
			  
			  for (Text val : values) {
				  if (val.toString().split(",")[0].equals("M")){//uisng file flag to identify which information to extract from mapper's result
					  name = val.toString().split(",")[1];
					  System.err.println(name);
				  }
				  else if (val.toString().split(",")[0].equals("F")){
					  String num = val.toString().split(",")[1];
					  number +=Integer.parseInt(num);
					  System.err.println(number);
					  
					  
				  }
				  
			  
			  
		}
			  Text results = new Text();
			  results.set(name+","+Integer.toString(number));
			  context.write(personID,results);

			  
	 }
	 }
		  

	 
		  public static void main(String[] args) throws Exception{
				// TODO Auto-generated method stub
				Configuration conf = new Configuration();
			    if (args.length != 3) {
			      System.err.println("Usage: queryd <HDFS input file1> <HDFS input file2> <HDFS output file>");
			      System.exit(3);
			    }
			    Job job = new Job(conf, "queryd");
			    job.setJarByClass(twod_nocombiner.class);
			    Path mypageInputPath = new Path(args[0]); //first input parameter
			    Path friendInputPath = new Path(args[1]); //second input parameter
			    Path outputPath = new Path(args[2]);//third input parameter
			    

			    job.setReducerClass(joinReducer.class);//set reducer

			    job.setOutputKeyClass(LongWritable.class);
			    job.setOutputValueClass(Text.class);
			    job.setInputFormatClass(TextInputFormat.class);

			    MultipleInputs.addInputPath(job, mypageInputPath,//link mypagemap to read MyPage
			            TextInputFormat.class, mypagemap.class);
			    MultipleInputs.addInputPath(job, friendInputPath,
			            TextInputFormat.class, friendmap.class);//link friendmap to read Friends
			    FileOutputFormat.setOutputPath(job, outputPath);
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
				}

			}