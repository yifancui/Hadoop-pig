package my;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
//We only use mapper to read file line by line

public class twoa {
    public static class facebookUserMapper extends Mapper<Object, Text, Text, NullWritable>{
        //private final static IntWritable one = new IntWritable(1);
        private Text text = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Consider to read each line first by using '\n'

            MyPage page = new MyPage(value.toString());
            if (page.getNationality().equals("LDPzI2fYhucyXJ")){
                text.set(page.getName() + "," + page.getHobby());
                context.write(text, null);
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: Facebook users whose Nationality is the same as your own Nationality <HDFS input file> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "task1");
        job.setJarByClass(twoa.class);
        job.setMapperClass(facebookUserMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(0);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}