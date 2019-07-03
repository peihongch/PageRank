package GraphBuilder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 输入：
 * 狄云 [戚芳,0.33333|戚长发,0.333333|卜垣,0.333333]
 * 戚芳 [狄云,0.25 |戚长发,0.25|卜垣,0.5]
 * 戚长发 [狄云,0.33333|戚芳,0.333333|卜垣,0.333333]
 * 卜垣 [狄云,0.25|戚芳,0.5|戚长发,0.25]
 */
public class GraphBuilderMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Pattern pattern = Pattern.compile("(\\S+)\\s\\[(.+)\\]");
        Matcher matcher = pattern.matcher(value.toString());
        while (matcher.find()) {
            String a = matcher.group(1);
            String b = matcher.group(2);
            // 默认初始PR是0.5
            context.write(new Text(a),
                    new Text("0.5-" + b));
        }
    }

    public static void main(String[] args) {
        Pattern pattern = Pattern.compile("(\\S+)\\s\\[(.+)\\]");
        Matcher matcher = pattern.matcher("狄云 [戚芳,0.33333|戚长发,0.333333|卜垣,0.333333]");
        while (matcher.find()) {
            String a = matcher.group(1);
            String b = matcher.group(2);
            System.out.println(a);
            System.out.println(b);
        }
    }
}
