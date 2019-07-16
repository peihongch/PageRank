package util;

import org.apache.hadoop.io.DoubleWritable;

public class DecDoubleWritable extends DoubleWritable {
    @Override
    public int compareTo(DoubleWritable o) {
        return -super.compareTo(o);
    }
}
