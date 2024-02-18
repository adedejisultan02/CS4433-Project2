import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {

    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {

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
                centroids.add(centroid);
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input data point
            String[] tokens = value.toString().split(",");
            double x = Double.parseDouble(tokens[0]);
            double y = Double.parseDouble(tokens[1]);

            // Find the nearest centroid
            int nearestCentroidIndex = 0;
            double minDistance = Double.MAX_VALUE;
            for (int i = 0; i < centroids.size(); i++) {
                double[] centroid = centroids.get(i);
                double distance = Math.sqrt(Math.pow(x - centroid[0], 2) + Math.pow(y - centroid[1], 2));
                if (distance < minDistance) {
                    minDistance = distance;
                    nearestCentroidIndex = i;
                }
            }

            // Emit the nearest centroid index and the data point
            context.write(new IntWritable(nearestCentroidIndex), value);
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Collect data points assigned to this centroid
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

            // Emit the new centroid
            context.write(key, new Text(newX + "," + newY));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("k", 3); // Number of clusters
        Job job = Job.getInstance(conf, "kmeans");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
