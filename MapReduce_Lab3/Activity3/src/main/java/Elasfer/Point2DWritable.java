package Elasfer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import java.awt.geom.Point2D;

public class Point2DWritable extends Point2D.Double implements Writable {
  
    public Point2DWritable() {
        super(0.0, 0.0);
    }

    public Point2DWritable(double x, double y) {
        super(x, y);
    }

    public Point2DWritable(Point2D.Double point) {
        super(point.x, point.y);
    }

    // Surdefinition des méthodes d'interface Writable
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(x);
        out.writeDouble(y);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x = in.readDouble();
        y = in.readDouble();
    }

    // Surdefinition de la méthode d'affichage d'output
    @Override
    public String toString() {
        return "(" + x + ", " + y + ")";
    }

    // Surdefinition des méthodes de comparaison
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Point2DWritable obj = (Point2DWritable) o;
        return java.lang.Double.compare(x, obj.x) == 0 && java.lang.Double.compare(y, obj.y) == 0;
    }

    @Override
    public int hashCode() {
        long xBits = java.lang.Double.doubleToLongBits(x);
        long yBits = java.lang.Double.doubleToLongBits(y);
        return (int) (xBits ^ (xBits >>> 32) ^ yBits ^ (yBits >>> 32));
    }
}