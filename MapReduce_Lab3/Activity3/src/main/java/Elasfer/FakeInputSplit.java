package Elasfer;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FakeInputSplit extends InputSplit implements Writable {
    private long numPoints;
    private long seed;
    
    // Default constructor required by Hadoop :
    public FakeInputSplit() {}
    
    public FakeInputSplit(long numPoints, long seed) {
        this.numPoints = numPoints;
        this.seed = seed;
    }
    // Override InputSplit methods :
    @Override
    public long getLength() {
        return numPoints * 16; // Approximate size in bytes (2 doubles per point)
    }
    
    @Override
    public String[] getLocations() {
        return new String[0]; // No locality preference (Synthetic data)
    }
    // Getters :
    public long getNumPoints() {
        return numPoints;
    }
    
    public long getSeed() {
        return seed;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(numPoints);
        out.writeLong(seed);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        numPoints = in.readLong();
        seed = in.readLong();
    }
}