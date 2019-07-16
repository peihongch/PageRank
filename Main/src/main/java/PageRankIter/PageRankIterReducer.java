package PageRankIter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 首先默认d为0.85
 */
public class PageRankIterReducer extends Reducer<Text, Text, Text, Text> {
    private double d;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        d = conf.getDouble("d", 0.85);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double rank = 0;
        StringBuilder linkList = new StringBuilder();
        for (Text value : values) {
            try {
                double pr = Double.valueOf(value.toString());
                rank += pr;
            } catch (Exception e) {
                linkList.append(value.toString());
            }
        }
        rank = (1 - d) + rank * d;
        linkList.insert(0, rank + "-");
        context.write(key, new Text(linkList.toString()));
    }
}
