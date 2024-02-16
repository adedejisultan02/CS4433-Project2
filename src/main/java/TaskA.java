import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
public class TaskA {
    public static class UserMapper extends Mapper<Object, Text, Text, Text> {
        private Text nationality = new Text();
        private Text nameHobby = new Text();
        private static final String TARGET_NATIONALITY = "Brunei"; // Set target nationality here

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] userData = value.toString().split(",");
            if (userData.length > 4 && userData[2].trim().equals(TARGET_NATIONALITY)) {
                nationality.set(userData[2].trim());
                nameHobby.set(userData[1].trim() + ", " + userData[4].trim());
                context.write(nationality, nameHobby);
            }
        }
    }

    public void debug(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        Path output = new Path(outputPath);

        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        Job job = Job.getInstance(conf, "User Nationality Report");

        job.setJarByClass(TaskA.class);
        job.setMapperClass(UserMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Nationality Report");

        job.setJarByClass(TaskA.class);
        job.setMapperClass(UserMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
