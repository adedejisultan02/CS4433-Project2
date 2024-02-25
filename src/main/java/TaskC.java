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

public class TaskC {
    static List<Double[]> centroids = new ArrayList<>();

    public static class KMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private List<double[]> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int k = conf.getInt("clusters", 1); // Number of clusters
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

    public static class KReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
        public static enum ConvergenceCounter { CONVERGED }
        private List<Double[]> oldCentroids = new ArrayList<>();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sumX = 0, sumY = 0, count = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                sumX += Double.parseDouble(parts[0]);
                sumY += Double.parseDouble(parts[1]);
                count++;
            }

            Double[] newCentroid = new Double[]{sumX / count, sumY / count};
            oldCentroids.add(newCentroid); // Update centroids

            // Check if centroids converge
            if (oldCentroids.size() > key.get()) {
                System.out.println("Checking convergence");
                Double[] oldCentroid = oldCentroids.get(key.get());
                double threshold = 0.001; // Adjust threshold as needed
                if (Math.abs(oldCentroid[0] - newCentroid[0]) <= threshold &&
                        Math.abs(oldCentroid[1] - newCentroid[1]) <= threshold) {
                    // Centroids haven't changed, terminate
                    context.getCounter(ConvergenceCounter.CONVERGED).increment(1);
                    context.setStatus("Centroids haven't changed. Early termination.");
                    return;
                }
            }

            // Emit new centroid
            context.write(NullWritable.get(),
                    new Text("Centroid " + key.get() + ": " + Arrays.toString(newCentroid)));
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        int maxIterations = Integer.parseInt(args[3]);
        int iteration = 0;
        boolean converged;
        while (iteration < maxIterations) {
            Path output = new Path(args[2] + "_" + iteration);
            if (hdfs.exists(output)) {
                hdfs.delete(output, true);
            }

            System.out.println("iteration " + iteration);
            conf.setInt("clusters", Integer.parseInt(args[1]));

            Job job = Job.getInstance(conf, "KMeans");

            job.setJarByClass(TaskC.class);
            job.setMapperClass(KMapper.class);
            job.setReducerClass(KReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, output);

            job.waitForCompletion(true);
            converged = job.getCounters().findCounter(KReducer.ConvergenceCounter.CONVERGED).getValue() > 0;

            // Check if converged
            if (converged) {
                System.out.println("Converged. K-Means clustering completed in " + (iteration + 1) + " iterations.");
                break; // Exit the loop if converged
            }

            // Clear old centroids for the next iteration
            centroids.clear();
            iteration++;
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        int maxIterations = 5;
        int iteration = 0;
        boolean converged;
        while (iteration < maxIterations) {
            Path output = new Path("output" + "_" + iteration);
            if (hdfs.exists(output)) {
                hdfs.delete(output, true);
            }

            System.out.println("iteration " + iteration);
            conf.setInt("clusters", 2);

            Job job = Job.getInstance(conf, "KMeans");

            job.setJarByClass(TaskC.class);
            job.setMapperClass(KMapper.class);
            job.setReducerClass(KReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path("dataset.csv"));
            FileOutputFormat.setOutputPath(job, output);

            job.waitForCompletion(true);
            converged = job.getCounters().findCounter(KReducer.ConvergenceCounter.CONVERGED).getValue() > 0;

            // Check if converged
            if (converged) {
                System.out.println("Converged. K-Means clustering completed in " + (iteration + 1) + " iterations.");
                break; // Exit the loop if converged
            }

            // Clear old centroids for the next iteration
            centroids.clear();
            iteration++;
        }
    }
}
