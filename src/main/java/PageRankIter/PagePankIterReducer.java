package PageRankIter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 首先默认d为0.85
 */
public class PagePankIterReducer extends Reducer<Text, Text, Text, Text> {
    private static final double d = 0.85;
    private int N;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // todo 从configuration获取节点数N
        N = 4;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
        rank = (1 - d) / N + rank * d;
        linkList.insert(0, rank + "-");
        context.write(key, new Text(linkList.toString()));
    }
}
