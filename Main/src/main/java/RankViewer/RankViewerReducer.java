package RankViewer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.DecDoubleWritable;

import java.io.IOException;

public class RankViewerReducer extends Reducer<DecDoubleWritable, Text, Text, DecDoubleWritable> {
    @Override
    protected void reduce(DecDoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
