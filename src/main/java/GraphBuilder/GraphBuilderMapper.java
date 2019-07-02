package GraphBuilder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ÊäÈë£º
 * µÒÔÆ [ÆÝ·¼,0.33333|ÆÝ³¤·¢,0.333333|²·Ô«,0.333333]
 * ÆÝ·¼ [µÒÔÆ,0.25 |ÆÝ³¤·¢,0.25|²·Ô«,0.5]
 * ÆÝ³¤·¢ [µÒÔÆ,0.33333|ÆÝ·¼,0.333333|²·Ô«,0.333333]
 * ²·Ô« [µÒÔÆ,0.25|ÆÝ·¼,0.5|ÆÝ³¤·¢,0.25]
 */
public class GraphBuilderMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Pattern pattern = Pattern.compile("(\\S+)\\s\\[(.+)\\]");
        Matcher matcher = pattern.matcher(value.toString());
        while (matcher.find()) {
            String a = matcher.group(1);
            String b = matcher.group(2);
            // Ä¬ÈÏ³õÊ¼PRÊÇ0.5
            context.write(new Text(a),
                    new Text("0.5-" + b));
        }
    }

    public static void main(String[] args) {
        Pattern pattern = Pattern.compile("(\\S+)\\s\\[(.+)\\]");
        Matcher matcher = pattern.matcher("µÒÔÆ [ÆÝ·¼,0.33333|ÆÝ³¤·¢,0.333333|²·Ô«,0.333333]");
        while (matcher.find()) {
            String a = matcher.group(1);
            String b = matcher.group(2);
            System.out.println(a);
            System.out.println(b);
        }
    }
}
