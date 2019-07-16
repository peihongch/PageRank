package RankViewer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.DecDoubleWritable;

import java.io.IOException;

public class RankViewerMapper extends Mapper<Text, Text, DecDoubleWritable, Text> {
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        DecDoubleWritable k = new DecDoubleWritable();
        k.set(Double.parseDouble(value.toString().split("-")[0]));
        context.write(k, key);
    }
}
