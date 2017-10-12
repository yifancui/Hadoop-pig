package my;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;






import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//by Xiaoting
public class twoh {
	static int sum=0;//Total number of friends for all the Facebook users
	static int number=0;//Number of facebook users in our dataset
	static Hashtable<Integer, Integer> count_table = new Hashtable<>();
	public static class friend1map//read in Friends dataset line by line
	   extends Mapper<LongWritable, Text, LongWritable, Text>{
		  private LongWritable personID = new LongWritable();
		  private Text friendInfo= new Text();
		  
		  public void map(LongWritable key, Text value, Context context
	                ) throws IOException, InterruptedException {
			  //System.err.println("map1");
			  String line = value.toString();
			  String[] splits = line.split(",");
			  personID.set(Long.parseLong(splits[1]));//get person id
			  
			  friendInfo.set(",1");
			  //System.err.println(",1");
			  context.write(personID, friendInfo);
			  
	}
	}
	//combiner take key(person id)-value(1) pairs from the dataset, calculating the total number of friends and facebook users 
	/*public static class friend1Reducer 
	 extends Reducer<LongWritable, Text,LongWritable,Text> {
		  
		  int groupnumber;//number of friends for each facebook user(for each person id)
		  public void reduce(LongWritable personID, Iterable<Text> values , 
	                 Context context
	                 ) throws IOException, InterruptedException {
			  groupnumber=0;
			  String perid="";
			  //System.err.println("reduce 1");
			  
			  for (Text val : values) {
				  perid= val.toString().split(",")[0];
				  groupnumber+=1;
				  sum+=1;
				  }
				  
			  number+=1;
			  Text results = new Text();
			  String num=Integer.toString(groupnumber);
			  
			  results.set(num);
			  
			  
			  context.write(personID,results);
		}
		  
		  
			  
	 
	 }*/
	//take key(person id)-value(number of friends for this person) pairs from combiner	  
	public static class friend2Reducer 
	 extends Reducer<LongWritable, Text,LongWritable,Text> {
		  String name;
		  int groupnumber;//total friends for a certain user
		  public void reduce(LongWritable personID, Iterable<Text> values , 
	                 Context context
	                 ) throws IOException, InterruptedException {
			  Text results = new Text();
			  LongWritable personid = new LongWritable();
			  //System.err.println("reducer2");
			  groupnumber=0;
			  String perid="";
			  
			  for (Text val : values) {
				  perid= val.toString().split(",")[0];
				  groupnumber+=1;
				  sum+=1;
				 
				  }
				  
			  number+=1;
			  //System.err.println(groupnumber+""+personID.get());
			 
			  //Text results = new Text();
			  //String num=Integer.toString(groupnumber);
			  
			 
			count_table.put((int) personID.get(),groupnumber);//put personID-groupnumber pairs into hashtable
			 
			  //results.set(num);
			  
			  
			  
			  //System.err.println(avg);
			  //results.set();
			  //personid.set(Long.parseLong(info[0]));
			  //context.write(personid,results);
			  
		
			  //System.err.println(count_table);
			  
	 }
		  protected void cleanup(Context context) throws IOException, InterruptedException {
			  System.err.println("clean");
			  if (count_table == null)
			  {
			       System.err.println("Filter cache was null");
			       return;
			  }
			  Iterator it=count_table.entrySet().iterator();
			  float avg=(float)sum/number;//calculate average number of friends
			  System.err.println(avg);
			 LongWritable personID = new LongWritable();
			  Text friendInfo= new Text();
			  
			  while(it.hasNext()){//read out the stored hashtable
				  Map.Entry<Integer,Integer> entry=(Entry<Integer, Integer>) it.next();
				  Object key =entry.getKey();
				  
				  Object value=entry.getValue();
				  Integer aa=Integer.parseInt(value.toString());
				  if(aa>avg){
					  Long person=Long.parseLong(key.toString());
					  personID.set(person);
					  friendInfo.set("This person is popular");
					  context.write(personID, friendInfo);
					  
				  }
				  
			  }

	        }
	 }
		  
	 
		  public static void main(String[] args) throws Exception{
				// TODO Auto-generated method stub
				Configuration conf = new Configuration();
			    if (args.length != 2) {
			      System.err.println("Usage: queryh <HDFS input file1> <HDFS input file2> <HDFS output file>");
			      System.exit(3);
			    }
			    Job job = new Job(conf, "twoh");
			    //String OUTPUT_PATH = "/home/yifan/hadoop/2job";
			    job.setJarByClass(twoh.class);
			    job.setMapperClass(friend1map.class);//set mapper
			   
			    //System.err.println("job1");
			    //job.setCombinerClass(friend1Reducer.class);//set combiner

			    job.setReducerClass(friend2Reducer.class);//set reducer

			    job.setOutputKeyClass(LongWritable.class);
			    job.setOutputValueClass(Text.class);
			    job.setInputFormatClass(TextInputFormat.class);

			    job.setOutputFormatClass(TextOutputFormat.class);
			    FileInputFormat.addInputPath(job, new Path(args[0]));
				
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				

				

				  /*
				   * Job 2
				   */
				//Configuration conf1 = new Configuration();
				  
				  System.exit(job.waitForCompletion(true) ? 0 : 1);

			

			}
}