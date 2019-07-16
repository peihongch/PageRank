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
public class PageRankIterMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("-");
        double curRank = Double.parseDouble(splits[0]);
        List<String> linkList = Arrays.asList(splits[1].split("\\|"));
        for (String link: linkList){
            Pattern pattern = Pattern.compile("(\\S+)[,](\\d+[\\.]\\d+)");
            Matcher matcher = pattern.matcher(link);
            while (matcher.find()){
                try {
                    context.write(new Text(matcher.group(1)),
                            new Text("" + curRank * Double.parseDouble(matcher.group(2))));
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                    System.out.println("输出PR值出错！");
                }
            }
        }
//        linkList.forEach(link -> {
//            Pattern pattern = Pattern.compile("(\\S+)[,](\\d+[\\.]\\d+)");
//            Matcher matcher = pattern.matcher(link);
//            while (matcher.find()){
//                try {
//                    context.write(new Text(matcher.group(1)),
//                            new Text("" + curRank * Double.parseDouble(matcher.group(2))));
//                } catch (IOException | InterruptedException e) {
//                    e.printStackTrace();
//                    System.out.println("输出PR值出错！");
//                }
//            }
//        });
        context.write(key, new Text(splits[1]));
    }
}
