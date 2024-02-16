import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskF {

    public static class FriendMapper extends Mapper<Object, Text, Text, Text> {
        private final Text personId = new Text();
        private final Text friendId = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] friendshipData = value.toString().split(",");
            String person = friendshipData[1];
            String friend = friendshipData[2];

            personId.set(person);
            friendId.set(friend);

            context.write(personId, new Text("F" + friendId));
        }
    }

    public static class AccessMapper extends Mapper<Object, Text, Text, Text> {

        private final Text personId = new Text();
        private final Text whatPage = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().startsWith("AccessId")) {
                return;
            }

            String[] accessData = value.toString().split(",");
            String byWho = accessData[1];
            String page = accessData[2];

            personId.set(byWho);
            whatPage.set(page);

            context.write(personId, new Text("A" + whatPage));
        }
    }

    public static class UnfollowedFriendsReducer extends Reducer<Text, Text, Text, Text> {

        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        private final Map<String, String> idToNameMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load page data from csv file
            try (BufferedReader reader = new BufferedReader(new FileReader("pages.csv"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        idToNameMap.put(parts[0], parts[1]); // Assuming the first column is PersonID and the second is Name
                    }
                }
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            HashSet<String> accessedPages = new HashSet<>();
            HashSet<String> friends = new HashSet<>();

            // Segregate based on the marker
            for (Text value : values) {
                String marker = value.toString().substring(0, 1);
                String content = value.toString().substring(1);

                if (marker.equals("A")) {
                    accessedPages.add(content);
                } else if (marker.equals("F")) {
                    friends.add(content);
                }
            }

            // Find friends who never accessed a page
            friends.removeAll(accessedPages);

            for (String friend : friends) {
                if (key.toString().contains("ByWho")) continue; //to avoid the header
                outputKey.set(key + " " + idToNameMap.get(key.toString()) + " unfollowed");
                outputValue.set("Friend ID: " + friend + " " + idToNameMap.get(friend));
                context.write(outputKey, outputValue);
            }
        }
    }


    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        Path output = new Path(args[3] + "_f");
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        Job job = Job.getInstance(conf, "Bad Friend");
        job.setJarByClass(TaskF.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,
                AccessMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,
                FriendMapper.class);
        job.setReducerClass(UnfollowedFriendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job, args[0] + "," + args[1]);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        Path output = new Path(args[2] + "_f");
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        Job job = Job.getInstance(conf, "Bad Friend");
        job.setJarByClass(TaskF.class);

        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,
                AccessMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class,
                FriendMapper.class);
        job.setReducerClass(UnfollowedFriendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job, args[1] + "," + args[2]);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}