package my;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//by Xiaoting
public class twog_nocombiner {

	//only use the AccessLog dataset, we map it to key:ByWho(person id),value:AccessTime
	//We initial a time unit, and we want to find out whoes last visit is before or on that day, which means they have not visit facebook since that day. 
	public static class accessmap
	   extends Mapper<LongWritable, Text, LongWritable, Text>{
		  private LongWritable personID = new LongWritable();
		  private Text accessInfo= new Text();
		  //read in file line by line
		  public void map(LongWritable key, Text value, Context context
	                ) throws IOException, InterruptedException {
			  //System.err.println("mapa");
			  String line = value.toString();//change each line to string
			  String[] splits = line.split(",");//seperate each line by comma
			  personID.set(Long.parseLong(splits[1]));//extract person id
			  String date = splits[4];//extract accesstime
			  //System.err.println(personID);
			  //System.err.println(date);
			  accessInfo.set(date);
			  context.write(personID, accessInfo);//write key(person id) value(accesstime) pairs
			  
	}
	}
	 
	//get key-value pairs from mapper, put them into reducer
	 public static class accessReducer 
	 extends Reducer<LongWritable, Text,LongWritable,Text> {
		  Integer lastvisit;//this number stands for last visit time 
		  
		  public void reduce(LongWritable personID, Iterable<Text> values , 
	                 Context context
	                 ) throws IOException, InterruptedException {
			  
			  lastvisit=0;//initial last visit for each person id
			  //System.err.println("reduce");
			  for (Text val : values) {
				  Integer date=Integer.parseInt(val.toString());
				  if (date>lastvisit){
					  lastvisit=date;//for each key find the max access time 
				  }  
					  
				  }
			  //System.err.println(lastvisit);
			  if(lastvisit<=900000){//if a person never visit facebook since 100 unit time, we think that is the one who has lost interest
				  Text results = new Text();
				  results.set("This person lost interest");
				  context.write(personID, results);
			  
		}	
			  }
		  }
			 
		  
	public static void main(String[] args) throws Exception{
	// TODO Auto-generated method stub
	Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.println("Usage: query1 <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    Job job = new Job(conf, "query");
    job.setJarByClass(twog_nocombiner.class);
    job.setMapperClass(accessmap.class);//Set mapper class
    //job.setCombinerClass(accessReducer.class);
    job.setReducerClass(accessReducer.class);//set reducer class
    //job.setNumReduceTasks(1);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));//input path
    FileOutputFormat.setOutputPath(job, new Path(args[1]));//output path
    System.exit(job.waitForCompletion(true) ? 0 : 1);//exit when complete
	}

}
