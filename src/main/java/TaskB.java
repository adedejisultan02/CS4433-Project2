import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.TreeMap;

public class popularPage     {

    int counter = 0;

    // Mapper class definition
    public static class AccessMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text pageID = new Text();
        private IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] userData = value.toString().split(",");
            if (userData.length > 4 && !userData[0].trim().equals("AccessID")) {
                pageID.set(userData[2].trim());
                context.write(pageID, one);
            }
        }
    }

    public static class AccessSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private TreeMap<Long, String> tmap;
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class top10PagesMapper extends Mapper<Object, Text, Text, Text> {

        PriorityQueue<Pair<Integer, String>> pq;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            pq = new PriorityQueue<>((a, b) -> a.getKey() - b.getKey());
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // input data format => page_id  count
            String[] pageAccesses = value.toString().split("\t");

            if (pageAccesses.length > 1) {
                String pageId = pageAccesses[0];
                int numAccesses = Integer.parseInt(pageAccesses[1]) * -1;

                pq.add(new Pair<>(numAccesses,pageId));
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            int desired_numbers = 10;
            for(int i=0; i<desired_numbers; i++)
            {
                int numAccessesOut = pq.peek().getKey();
                String pageIDout = pq.peek().getValue();
                pq.remove();

                context.write(new Text(Integer.toString(numAccessesOut * -1)), new Text(pageIDout));
            }
        }
    }


    public static class CsvJoinMapper extends Mapper<Object, Text, Text, NullWritable> {

        private Text pageId = new Text();
        private Text pageData = new Text();
        private HashSet<String> pageIDs = new HashSet<String>();
        private IntWritable one = new IntWritable(1);

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path path = new Path(cacheFiles[0]);
                // open the stream
                FileSystem fs = FileSystem.get(context.getConfiguration());
                FSDataInputStream fis = fs.open(path);
                // wrap it into a BufferedReader object which is easy to read a record
                BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
                // read the record line by line
                String line;
                while (StringUtils.isNotEmpty(line = reader.readLine())) {
                    String[] split = line.split("\t");
                    pageIDs.add(split[1]);
                }
                // close the stream
                IOUtils.closeStream(reader);
            } else {
                System.out.println("No cache files found!");
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (pageIDs.contains(fields[0])) {
                pageData.set(fields[0] + ", " + fields[1] + ", " + fields[2]);
                context.write(pageData, NullWritable.get());
            }
        }
    }


    public void debug(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        FileSystem hdfs = FileSystem.get(conf1);
        Path output = new Path("output");
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        // JOB 1
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Page Indexer");

        job1.setJarByClass(popularPage.class);
        job1.setMapperClass(AccessMapper.class);
        job1.setReducerClass(AccessSumReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2] + "/accessTotal"));
        job1.waitForCompletion(true);

        // JOB 2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Top 10 fixer");
        job2.setJarByClass(popularPage.class);
        job2.setMapperClass(top10PagesMapper.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[2] + "/accessTotal"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/top10partial"));
        job2.waitForCompletion(true);

        // JOB 3
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3);
        job3.addCacheFile(new URI("output/top10partial/part-r-00000"));
        job3.setJarByClass(popularPage.class);
        job3.setMapperClass(CsvJoinMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(NullWritable.class);
        // a file in local file system is being used here as an example
        // set the number of reduceTask to 0 becuase this is a map-only job
        job3.setNumReduceTasks(0);
        // change the following input/output path based on your need
        FileInputFormat.setInputPaths(job3, new Path(args[1]));
        FileOutputFormat.setOutputPath(job3, new Path(args[2] + "/top10Final"));
        job3.waitForCompletion(true);

    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        FileSystem hdfs = FileSystem.get(conf1);
        Path output = new Path("output");
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        // JOB 1
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Page Indexer");

        job1.setJarByClass(popularPage.class);
        job1.setMapperClass(AccessMapper.class);
        job1.setReducerClass(AccessSumReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2] + "/accessTotal"));
        job1.waitForCompletion(true);

        // JOB 2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Top 10 fixer");
        job2.setJarByClass(popularPage.class);
        job2.setMapperClass(top10PagesMapper.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[2] + "/accessTotal"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/top10partial"));
        job2.waitForCompletion(true);

        // JOB 3
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3);
        job3.addCacheFile(new URI("output/top10partial/part-r-00000"));
        job3.setJarByClass(popularPage.class);
        job3.setMapperClass(CsvJoinMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(NullWritable.class);
        job3.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job3, new Path(args[1]));
        FileOutputFormat.setOutputPath(job3, new Path(args[2] + "/top10Final"));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }


}