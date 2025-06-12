package Elasfer;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RandomPointGeneratorJob {
    // Mapper class :
    public class PointGeneratorMapper extends Mapper<LongWritable, Point2DWritable, Text, Text> {
    
        private Text outputKey = new Text();
        private Text outputValue = new Text();
    
        @Override
        protected void map(LongWritable key, Point2DWritable value, Context context) throws IOException, InterruptedException {
            // Convert point to string format "x,y"
            String pointStr = value.getX() + "," + value.getY();
        
            // Use the point index as key and point coordinates as value
            outputKey.set(String.valueOf(key.get()));
            outputValue.set(pointStr);
            // Write the key-value pair to context
            context.write(outputKey, outputValue);
        }
    }
    // Reducer class :
    public class PointFileWriterReducer extends Reducer<Text, Text, Text, Text> {
    
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Simply write each point to the output file (each point in its own text file)
            for (Text value : values) {
                // Write the point to the context with a null key
                context.write(null, value); // null key to avoid redundant keys in output
            }
        }
    }
    
    // Main method to set up and run the job
    public static void main(String[] args) throws Exception {
        // This job expects three arguments:
        if (args.length != 3) {
            System.err.println("Usage: RandomPointGeneratorJob <output_path> <num_points> <num_splits>");
            System.exit(1);
        }
        // First argument is output path
        String outputPath = args[0];
        // Second is number of points
        long numPoints = Long.parseLong(args[1]);
        // Third is number of splits
        int numSplits = Integer.parseInt(args[2]);
        // Create a configuration object
        Configuration conf = new Configuration();
        // Set the number of points and splits in the configuration
        conf.setLong(RandomPointInputFormat.NUM_POINTS, numPoints);
        conf.setInt(RandomPointInputFormat.NUM_SPLITS, numSplits);
        // Create a job instance with the configuration
        Job job = Job.getInstance(conf, "Random Point Generator");
        job.setJarByClass(RandomPointGeneratorJob.class);
        // Set input format to our random point generator
        job.setInputFormatClass(RandomPointInputFormat.class);
        // Mapper configuration :
        job.setMapperClass(PointGeneratorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // Reducer configuration :
        job.setReducerClass(PointFileWriterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Output path configuration :
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // Exit when the job is completed
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
