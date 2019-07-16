package LPA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 输入：
 * 人物:标签|有关人物1:标签1,权重1;有关人物2:标签2,权重2;...
 * <p>
 * Map阶段：
 * 对于输入文件中每条记录，记录所有标签对应的权重（如果多个任务对应同一个标签的话，该标签的权重是所有权重之和）,
 * 然后从中选取权重最高的作为人物的标签。此时我们得到了一系列<人物:标签>，将其作为输出返回给Reduce
 */
public class LabelPropagationMapper extends Mapper<Object, Text, Text, Text> {
    private Logger logger;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger = LoggerFactory.getLogger(LabelPropagationMapper.class);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String name = "", links = "";

        logger.info("value: " + value.toString());
        Pattern pattern = Pattern.compile("(\\S+)\\|(\\S+)");
        Matcher matcher = pattern.matcher(value.toString());
        logger.info("start finding...");
        while (matcher.find()) {
            name = matcher.group(1);
            links = matcher.group(2);
        }
        logger.info("end finding...");
        logger.info("nameAndTag: " + name);
        logger.info("links: " + links);
        name = name.split("[:]")[0];
        context.write(new Text(name), new Text(links));

        Configuration conf = context.getConfiguration();
        // 累计标签的权重
        Hashtable<String, Double> tagWeightTable = new Hashtable<>();
        for (String link:links.split("[;]")){
            Pattern p = Pattern.compile("(\\S+):(\\S+)[,](\\S+)");
            Matcher m = p.matcher(link);
            while (m.find()) {
                Double weight = tagWeightTable.get(m.group(2));
                if (weight == null) {
                    tagWeightTable.put(m.group(2), Double.parseDouble(m.group(3)));
                } else {
                    tagWeightTable.put(m.group(2), weight + Double.parseDouble(m.group(3)));
                }
            }
        }
//        Arrays.asList(links.split("[;]")).forEach(link -> {
//            Pattern p = Pattern.compile("(\\S+):(\\S+)[,](\\S+)");
//            Matcher m = p.matcher(link);
//            while (m.find()) {
//                Double weight = tagWeightTable.get(m.group(2));
//                if (weight == null) {
//                    tagWeightTable.put(m.group(2), Double.parseDouble(m.group(3)));
//                } else {
//                    tagWeightTable.put(m.group(2), weight + Double.parseDouble(m.group(3)));
//                }
//            }
//        });

        // 寻找权重最大的标签
        Iterator iterator = tagWeightTable.keySet().iterator();
        String maxLabel = "";
        Double maxWeight = 0.0;
        while (iterator.hasNext()) {
            String label = (String) iterator.next();
            Double weight = tagWeightTable.get(label);
            if (weight > maxWeight) {
                maxWeight = weight;
                maxLabel = label;
            }
        }

        logger.info("==================================================================");
        logger.info("start setConf...");
        logger.info("name: " + name);
        logger.info("label: " + maxLabel);
        // 将更新后的标签写入Configuration以便全部的Reducer都能够访问
        conf.set(name, maxLabel);
        logger.info("get label from conf: " + conf.get(name));
        logger.info("end setConf...");
    }

    public static void main(String[] args) {
        Pattern pattern = Pattern.compile("(\\S+):(\\S+)\\|(\\S+)");
        Matcher matcher = pattern.matcher("人物:标签|有关人物1:标签1,权重1;有关人物2:标签2,权重2");
        Logger logger = LoggerFactory.getLogger(LabelPropagationMapper.class);
        logger.info("start finding...");
        while (matcher.find()) {
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(3));
        }
    }
}
