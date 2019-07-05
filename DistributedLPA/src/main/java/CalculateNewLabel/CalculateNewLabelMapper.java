package CalculateNewLabel;

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

public class CalculateNewLabelMapper extends Mapper<Object, Text, Text, Text> {
    private Logger logger;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger = LoggerFactory.getLogger(CalculateNewLabelMapper.class);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String name = "", links = "";

        logger.info("value: " + value.toString());
        Pattern pattern = Pattern.compile("(\\S+)[\\||\\s](\\S+)");
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

        // 累计标签的权重
        Hashtable<String, Double> tagWeightTable = new Hashtable<>();
        Arrays.asList(links.split("[;]")).forEach(link -> {
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
        });

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
        logger.info("start emitting...");
        logger.info("name: " + name);
        logger.info("label: " + maxLabel);
        // 将更新后的标签输出
        context.write(new Text(name), new Text(maxLabel));
        logger.info("end emitting...");
    }

    public static void main(String[] args) {
        Pattern pattern = Pattern.compile("(\\S+)[\\||\\s](\\S+)");
        Matcher matcher = pattern.matcher("一灯大师:黄蓉\t丘处机:郭靖,0.012106;乔寨主:汉子,0.002421;农夫:黄蓉,0.041162;华筝:郭靖,0.");
        while (matcher.find()) {
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
        }
    }
}
