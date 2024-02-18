import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.*;

public class kMeansOptimized {

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {

        private List<String> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    centroids.add(line);
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] pointCoords = value.toString().split(",");
            int pointX = Integer.parseInt(pointCoords[0]);
            int pointY = Integer.parseInt(pointCoords[1]);

            double minDistance = Double.POSITIVE_INFINITY;
            String closestCentroid = null;

            for (String centroid : centroids) {
                String[] centroidCoords = centroid.split(",");
                int centroidX = Integer.parseInt(centroidCoords[0]);
                int centroidY = Integer.parseInt(centroidCoords[1]);

                double distance = Math.sqrt(Math.pow(pointX - centroidX, 2) + Math.pow(pointY - centroidY, 2));

                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroid = centroid;
                }
            }

            context.write(new Text(closestCentroid), new Text(pointX + "," + pointY + ",1"));
        }
    }

    public static class KMeansCombiner extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumX = 0;
            int sumY = 0;
            int count = 0;

            for (Text value : values) {
                String[] pointCoords = value.toString().split(",");
                sumX += Integer.parseInt(pointCoords[0]);
                sumY += Integer.parseInt(pointCoords[1]);
                count += Integer.parseInt(pointCoords[2]);
            }

            context.write(key, new Text(sumX + "," + sumY + "," + count));
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumX = 0;
            int sumY = 0;
            int totalCount = 0;

            for (Text value : values) {
                String[] pointCoords = value.toString().split(",");
                sumX += Integer.parseInt(pointCoords[0]);
                sumY += Integer.parseInt(pointCoords[1]);
                totalCount += Integer.parseInt(pointCoords[2]);
            }

            int newCentroidX = sumX / totalCount;
            int newCentroidY = sumY / totalCount;

            context.write(new Text(newCentroidX + "," + newCentroidY), new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: KMeansOptimized <centroids_file> <input_dataset> <output_directory>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans Optimized");

        job.setJarByClass(kMeansOptimized.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class);
        job.setReducerClass(KMeansReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(args[0])); // args[0] contains the path to the centroids file

        FileInputFormat.addInputPath(job, new Path(args[1])); // args[1] contains the path to the input dataset
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // args[2] contains the path to the output directory

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
