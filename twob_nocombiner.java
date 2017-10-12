package my;



import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * By Han Jiang on 9/17/2017
 */

public class twob_nocombiner {
//We use one mapper to read file line by line
    public static class countryMapper extends Mapper<Object, Text, Text, IntWritable>{

        private IntWritable count = new IntWritable(1);
        private Text text = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        	String line = value.toString();//change line to string
			String[] splits = line.split(",");//split line
        	count.set(1);//set one to count
            text.set(splits[3]);//take countrycode
            context.write(text,count);//write key:name(text)-value:1(int) pairs

        }
    }//reducer get key-value pairs 
    public static class countryCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;//initial sum to record number of "1" for each key
            for (IntWritable val : values){
                sum += val.get();//get the number
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: Facebook users whose Nationality is the same as your own Nationality <HDFS input file> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "task2b");
        job.setJarByClass(twob_nocombiner.class);
        job.setMapperClass(countryMapper.class);//set mapper
        //job.setCombinerClass(countryCountReducer.class);
        job.setReducerClass(countryCountReducer.class);//set reducer
        job.setOutputKeyClass(Text.class);//output key:text
        job.setOutputValueClass(IntWritable.class);//output value:int
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        //job.setNumReduceTasks(0);
    }


}