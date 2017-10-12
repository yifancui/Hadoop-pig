package my;

import java.io.IOException;
import java.util.*;

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
 * By Han Jiang on 9/18/2017
 */

public class twoc_nocombiner {
//mapper to read file line by line
    public static class accessMapper extends Mapper<Object, Text, IntWritable, IntWritable>{

        private IntWritable count = new IntWritable(1);//set "1"
        private IntWritable ID = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            AccessLog log = new AccessLog(value.toString());//change line to string
            ID.set(log.getWhatPage());//get page id
            context.write(ID,count);

        }
    }//reducer to get key-value pairs
    public static class accessReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        private IntWritable result1 = new IntWritable();
        private IntWritable result2 = new IntWritable();
        private Hashtable<Integer, Integer> count_table;

        protected void setup(Context context) throws IOException, InterruptedException {
            count_table = new Hashtable<>();//create a hashtable

        }


        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;//Initialize number of access
            for (IntWritable val : values){
                sum += val.get();//get number of access
            }


            count_table.put(key.get(),sum);//set key-value
            //System.err.println(count_table);
            //context.write(key, result);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
        	System.err.println("clean");

            CountComparartor cc = new CountComparartor();
            PriorityQueue<Map.Entry<Integer, Integer>> pq = new PriorityQueue<>(10, cc);//largest 10 items
            pq.addAll(count_table.entrySet());

            int i = 0;
            while(pq.size() != 0 && i <= 9){
                Map.Entry<Integer, Integer> map = pq.remove();//remove smallest each time

                result1.set(map.getKey());
                result2.set(map.getValue());
                context.write(result1, result2);
                
                i++;
            }

        }

        private static class CountComparartor implements Comparator<Map.Entry<Integer, Integer>> {
            public int compare(Map.Entry<Integer, Integer> map1, Map.Entry<Integer, Integer> map2){
                if(map1.getValue() < map2.getValue()){//compare values
                    return 1;
                }
                if(map1.getValue() > map2.getValue()){
                    return -1;
                }
                return 0;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: top 10 interesting Facebook pages <HDFS input file> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "task2c");
        job.setJarByClass(twoc_nocombiner.class);
        job.setMapperClass(accessMapper.class);//set mapper
        //job.setCombinerClass(accessReducer.class);
        job.setReducerClass(accessReducer.class);//set reducer
        job.setOutputKeyClass(IntWritable.class);//output key int
        job.setOutputValueClass(IntWritable.class);//output value int
        FileInputFormat.addInputPath(job, new Path(args[0]));//input path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//output path
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}