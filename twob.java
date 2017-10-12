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

public class twob {

    public static class countryMapper extends Mapper<Object, Text, Text, IntWritable>{

        private IntWritable count = new IntWritable(1);
        private Text text = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

          	String line = value.toString();//change line to string
        			String[] splits = line.split(",");
                	count.set(1);
                    text.set(splits[3]);
                    context.write(text,count);


        }
    }
    public static class countryCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values){
                sum += val.get();
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
        job.setJarByClass(twob.class);
        job.setMapperClass(countryMapper.class);
        job.setCombinerClass(countryCountReducer.class);
        job.setReducerClass(countryCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        //job.setNumReduceTasks(0);
    }


}