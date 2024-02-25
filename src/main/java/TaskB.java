import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TaskB {

    public static class KMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private List<double[]> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int k = conf.getInt("k", 1); // Number of clusters
            Random rand = new Random();
            for (int i = 0; i < k; i++) {
                double[] centroid = new double[2]; // Assuming 2D data
                centroid[0] = rand.nextDouble() * 100; // Random initialization
                centroid[1] = rand.nextDouble() * 100; // Random initialization
                System.out.println(Arrays.toString(centroid));
                centroids.add(centroid);
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            double x = Double.parseDouble(parts[0]);
            double y = Double.parseDouble(parts[1]);

            // Find closest centroid
            double minDist = Double.MAX_VALUE;
            int closestCentroidIndex = -1;
            for (int i = 0; i < centroids.size(); i++) {
                double dist = Math.sqrt(Math.pow(x - centroids.get(i)[0], 2) + Math.pow(y - centroids.get(i)[1], 2));
                if (dist < minDist) {
                    minDist = dist;
                    closestCentroidIndex = i;
                }
            }

            // Emit point and corresponding centroid index
            context.write(new IntWritable(closestCentroidIndex), value);
        }
    }

    public static class KReducer extends Reducer<IntWritable, Text, IntWritable, Text> {



        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<double[]> points = new ArrayList<>();
            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                double x = Double.parseDouble(tokens[0]);
                double y = Double.parseDouble(tokens[1]);
                points.add(new double[]{x, y});
            }

            // Calculate the new centroid
            double sumX = 0, sumY = 0;
            for (double[] point : points) {
                sumX += point[0];
                sumY += point[1];
            }
            double newX = sumX / points.size();
            double newY = sumY / points.size();
            System.out.println(newX + " " + newY);

            // Emit the new centroid
            context.write(key, new Text(newX + "," + newY));
        }
    }

    public void debug(String[] args) throws Exception {


        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        int maxIterations = Integer.parseInt(args[3]);
        int iteration;
        for (iteration = 0; iteration < maxIterations; iteration++) {
            Path output = new Path(args[2]+"_"+iteration);
            if (hdfs.exists(output)) {
                hdfs.delete(output, true);
            }
            System.out.println("iteration " + iteration);
            conf.setInt("kmeans.iteration", iteration);

            Job job = Job.getInstance(conf, "KMeans");

            job.setJarByClass(TaskB.class);
            job.setMapperClass(KMapper.class);
            job.setReducerClass(KReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, output);

            job.waitForCompletion(true);
        }
        System.out.println("K-Means clustering completed in " + (iteration) + " iterations.");
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        int maxIterations = Integer.parseInt(args[3]);
        int iteration;
        for (iteration = 0; iteration < maxIterations; iteration++) {
            Path output = new Path(args[2]+"_"+iteration);
            if (hdfs.exists(output)) {
                hdfs.delete(output, true);
            }
            System.out.println("iteration " + iteration);

            Job job = Job.getInstance(conf, "KMeans");

            job.setJarByClass(TaskB.class);
            job.setMapperClass(KMapper.class);
            job.setReducerClass(KReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, output);

            job.waitForCompletion(true);
        }
        System.out.println("K-Means clustering completed in " + (iteration) + " iterations.");
    }
}
