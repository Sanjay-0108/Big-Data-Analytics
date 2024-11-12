import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ClassWiseToppers {

    // Mapper for Name Dataset (ID, Name)
    public static class NameMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("");
            
            // This should be for the name dataset
            if (fields.length == 2) {
                String id = fields[0];
                String name = fields[1];
                context.write(new Text(id), new Text("NAME#" + name));
            }
        }
    }

    // Mapper for Marks Dataset (ID, Course, Score1, Score2, Score3, Location)
    public static class MarksMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");

            // This should be for the marks dataset
            if (fields.length == 6) {
                String id = fields[0];
                String course = fields[1];
                int score1 = Integer.parseInt(fields[2]);
                int score2 = Integer.parseInt(fields[3]);
                int score3 = Integer.parseInt(fields[4]);
                int totalScore = score1 + score2 + score3;
                context.write(new Text(id), new Text("MARKS#" + course + "#" + totalScore));
            }
        }
    }

    // Reducer Class
    public static class TopperReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, String> toppers = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = "";
            String course = "";
            int totalScore = 0;

            // Separate the name and marks information for each key
            for (Text val : values) {
                String[] parts = val.toString().split("#");
                if (parts[0].equals("NAME")) {
                    name = parts[1];
                } else if (parts[0].equals("MARKS")) {
                    course = parts[1];
                    totalScore = Integer.parseInt(parts[2]);
                }
            }

            // Update the top scorer details for each course
            if (!course.isEmpty()) {
                if (!toppers.containsKey(course) || totalScore > Integer.parseInt(toppers.get(course).split("\t")[2])) {
                    toppers.put(course, key.toString() + "\t" + name + "\t" + totalScore);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the top scorer for each class
            for (Map.Entry<String, String> entry : toppers.entrySet()) {
                String course = entry.getKey();
                String details = entry.getValue();
                context.write(new Text(course), new Text(details));
            }
        }
    }

    // Driver Code
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ClassWiseToppers");

        job.setJarByClass(ClassWiseToppers.class);
        job.setReducerClass(TopperReducer.class);

        // Set map output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set final output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Use MultipleInputs to read both datasets
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, NameMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MarksMapper.class);
        
        // Use FileInputFormat to process the input files and setup the input paths
        FileInputFormat.addInputPath(job, new Path(args[0])); // For the first dataset (names)
        FileInputFormat.addInputPath(job, new Path(args[1])); // For the second dataset (marks)

        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
