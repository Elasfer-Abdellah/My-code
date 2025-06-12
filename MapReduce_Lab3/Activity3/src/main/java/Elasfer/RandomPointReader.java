package Elasfer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;
import java.util.Random;

public class RandomPointReader extends RecordReader<LongWritable, Point2DWritable> {
    // Key and value types for the RecordReader :
    private LongWritable key = new LongWritable();
    private Point2DWritable value = new Point2DWritable();
    // Random number to create a seed
    private Random random;
    // Number of points generated :
    private long pointsGenerated = 0;
    // Total number of points to generate :
    private long totalPoints;
    // Override RecordReader methods :
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        // create a FakeInputSplit object to get the number of points and seed
        FakeInputSplit fakeSplit = (FakeInputSplit) split;
        this.totalPoints = fakeSplit.getNumPoints();
        this.random = new Random(fakeSplit.getSeed());
    }
    
    @Override
    public boolean nextKeyValue() throws IOException {
        // Generate points until we reach the total number of points
        if (pointsGenerated >= totalPoints) {
            return false;
        }
        // key is the current point number
        key.set(pointsGenerated);
        // value is a new Point2DWritable with random coordinates
        value.setLocation(random.nextDouble() * 100, random.nextDouble() * 100);
        pointsGenerated++;
        return true;
    }
    
    @Override
    public float getProgress() {
        // return the progress as a fraction of points generated over total points
        return totalPoints == 0 ? 0 : (float) pointsGenerated / totalPoints;
    }
    
    @Override
    public void close() throws IOException {
        // No resources to clean up
    }
    // Getters :
    @Override
    public LongWritable getCurrentKey() {
        return key;
    }
    
    @Override
    public Point2DWritable getCurrentValue() {
        return value;
    }
}
