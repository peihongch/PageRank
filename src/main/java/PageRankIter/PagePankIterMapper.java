package PageRankIter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 根据权重给不同的链接节点分配PR值
 */
public class PagePankIterMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("-");
        double curRank = Double.parseDouble(splits[0]);
        List<String> linkList = Arrays.asList(splits[1].split("\\|"));
        linkList.forEach(link -> {
            Pattern pattern = Pattern.compile("(\\S+)[,](\\d+[\\.]\\d+)");
            Matcher matcher = pattern.matcher(link);
            while (matcher.find()){
                try {
                    context.write(new Text(matcher.group(1)), new Text("" + curRank * Double.parseDouble(matcher.group(2))));
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                    System.out.println("输出PR值出错！");
                }
            }
        });
        context.write(key, new Text(splits[1]));
    }

    public static void main(String[] args) {
        String[] splits = "0.5-戚芳,0.33333|戚长发,0.333333|卜垣,0.333333".split("-");
        double curRank = Double.parseDouble(splits[0]);
        List<String> linkList = Arrays.asList(splits[1].split("\\|"));
        linkList.forEach(link -> {
            Pattern pattern = Pattern.compile("(\\S+)[,](\\d+[\\.]\\d+)");
            Matcher matcher = pattern.matcher(link);
            while (matcher.find()){
                System.out.println(matcher.group(1));
                System.out.println(curRank * Double.parseDouble(matcher.group(2)));
            }
        });
        System.out.println(splits[1]);
    }
}
