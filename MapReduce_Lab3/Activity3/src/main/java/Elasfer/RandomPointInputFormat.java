package Elasfer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomPointInputFormat extends InputFormat<LongWritable, Point2DWritable> {
    public static final String NUM_POINTS = "random.points.total";
    public static final String NUM_SPLITS = "random.points.splits";
    
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        // Total number of points to generate
        long totalPoints = job.getConfiguration().getLong(NUM_POINTS, 1000);
        // Number of splits to create
        int numSplits = job.getConfiguration().getInt(NUM_SPLITS, 10);
        // List of all the splits to be returned
        List<InputSplit> splits = new ArrayList<>(numSplits);
        // Random number to generate the seed for each split
        Random rand = new Random();
        // Number of points per split
        long pointsPerSplit = totalPoints / numSplits;
        // Remainder points to distribute among the splits
        long remainder = totalPoints % numSplits;
        // Generate the splits
        for (int i = 0; i < numSplits; i++) {
            // Each split gets a random seed
            long pointsInThisSplit = pointsPerSplit + (i < remainder ? 1 : 0);
            splits.add(new FakeInputSplit(pointsInThisSplit, rand.nextLong()));
        }
        // Return the list of splits
        return splits;
    }
    // Create a RecordReader for each split
    @Override
    public RecordReader<LongWritable, Point2DWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new RandomPointReader();
    }
}
