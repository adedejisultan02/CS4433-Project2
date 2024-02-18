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

public class KMultiIteration {

    public static class KMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private final List<Double[]> centroids = new ArrayList<>();


        @Override
        protected void setup(Mapper.Context context) throws InterruptedException {
            System.out.println("Setting up mapper");

            try (BufferedReader br = new BufferedReader(new FileReader("seeds.csv"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    centroids.add(new Double[]{Double.parseDouble(parts[0]), Double.parseDouble(parts[1])});
                    System.out.println("value of centroid " + Arrays.toString(centroids.get(centroids.size() - 1)));
                }
                br.close();

                for (int i = 0; i < centroids.size(); i++) {
                    System.out.println("Centroid " + i + ": " + Arrays.toString(centroids.get(i)));
                }
            } catch (IOException e) {
                System.err.println("Error reading centroids file: " + e.getMessage());
                throw new InterruptedException("Error reading centroids file");
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

        private final List<Double[]> oldCentroids = new ArrayList<>();

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
            oldCentroids.add(newCentroid);



            // Update old centroids
            if (oldCentroids.size() <= key.get()) {
                while (oldCentroids.size() <= key.get()) {
                    oldCentroids.add(null);
                }
            }

            // Write new centroid
            context.write(NullWritable.get(),
                    new Text("Centroid " + key.get() + ": " + Arrays.toString(newCentroid)));
        }
    }

    public void debug(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Not enough arguments");
            System.exit(1);
        }

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

            job.setJarByClass(KMultiIteration.class);
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
        }
        System.out.println("K-Means clustering completed in " + (iteration) + " iterations.");
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Not enough arguments");
            System.exit(1);
        }

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

            job.setJarByClass(KMultiIteration.class);
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
        }
        System.out.println("K-Means clustering completed in " + (iteration) + " iterations.");
    }
}
