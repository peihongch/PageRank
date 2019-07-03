package LPA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * value:
 * 有关人物1:标签1,权重1;有关人物2:标签2,权重2
 */
public class LabelPropagationReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        for (Text links : values) {
            // 替换标签
            List<String> newLinks = new ArrayList<>();
            Arrays.asList(links.toString().split("[;]")).forEach(link -> {
                String[] splits = link.split("[:,]");
                String newTag = conf.get(splits[0]);
                newLinks.add(splits[0] + ":" + newTag + "," + splits[2]);
            });

            // 拼接成新的链接
            String value = newLinks.stream().reduce((link1, link2) -> link1 + ";" + link2).orElse("");

            String nameAndTag = key.toString();
            nameAndTag = nameAndTag + ":" + conf.get(nameAndTag);
            context.write(new Text(nameAndTag), new Text(value));
        }
    }
}
