package GraphBuilder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ÊäÈë£º
 * µÒÔÆ [ÆÝ·¼,0.33333|ÆÝ³¤·¢,0.333333|²·Ô«,0.333333]
 * ÆÝ·¼ [µÒÔÆ,0.25 |ÆÝ³¤·¢,0.25|²·Ô«,0.5]
 * ÆÝ³¤·¢ [µÒÔÆ,0.33333|ÆÝ·¼,0.333333|²·Ô«,0.333333]
 * ²·Ô« [µÒÔÆ,0.25|ÆÝ·¼,0.5|ÆÝ³¤·¢,0.25]
 */
public class GraphBuilderMapper extends Mapper<Text, Text,Text,Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        // Ä¬ÈÏ³õÊ¼PRÊÇ0.5
        context.write(new Text(splits[0]),
                new Text("0.5-"+splits[1].substring(1,splits[1].length()-1)));
    }
}
