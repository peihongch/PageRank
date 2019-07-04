package LPA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * value:
 * 有关人物1:标签1,权重1;有关人物2:标签2,权重2
 */
public class LabelPropagationReducer extends Reducer<Text, Text, Text, Text> {
    private Logger logger;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger = LoggerFactory.getLogger(LabelPropagationReducer.class);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        logger.info("key: " + key.toString());
        for (Text links : values) {
            logger.info("value: " + links.toString());
            // 替换标签
            List<String> newLinks = new ArrayList<>();
            Arrays.asList(links.toString().split("[;]")).forEach(link -> {
                logger.info("links after split...");
                logger.info("link: " + link);
                Pattern p = Pattern.compile("(\\S+)[,](\\S+)");
                Matcher m = p.matcher(link);
                while (m.find()) {
                    logger.info("nameAndTag: " + m.group(1));
                    logger.info("weight: " + m.group(2));
                    String name = m.group(1).split(":")[0];
                    String newTag = conf.get(name);
                    newLinks.add(name + ":" + newTag + "," + m.group(2));
                }
            });
            logger.info("==================================================");
            logger.info("newLinks: ");
            for (String s : newLinks) logger.info(s);
            logger.info("==================================================");

            // 拼接成新的链接
            String value = newLinks.stream().reduce((link1, link2) -> link1 + ";" + link2).orElse("");
            logger.info("拼接成新的链接 newLinks: " + value);

            String nameAndTag = key.toString();
            nameAndTag = nameAndTag + ":" + conf.get(nameAndTag);
            context.write(new Text(nameAndTag), new Text(value));
        }
    }
}
