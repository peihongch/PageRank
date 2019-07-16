package UpdateLabel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UpdateLabelMapper extends Mapper<Object, Text, Text, Text> {
    private Logger logger;
    private Hashtable<String, String> newTags;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger = LoggerFactory.getLogger(UpdateLabelMapper.class);
        newTags = new Hashtable<>();

        logger.info("开始加载更新后的标签......");
        Configuration conf = context.getConfiguration();
        Path[] path = DistributedCache.getLocalCacheFiles(conf);
        logger.info("获取的路径是：  " + path[0].toString());
        FileSystem fsopen = FileSystem.getLocal(conf);
        FSDataInputStream in = fsopen.open(path[0]);
        Scanner scan = new Scanner(in);
        while (scan.hasNext()) {
            String[] tags = scan.nextLine().split("\\s");
            newTags.put(tags[0], tags[1]);
        }
        scan.close();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        logger.info("key: " + key.toString());
        logger.info("value: " + value.toString());

        String name = "", links = "";
        Pattern pattern = Pattern.compile("(\\S+)[\\|\\s](\\S+)");
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
        logger.info("name: " + name);

        // 替换标签
        List<String> newLinks = new ArrayList<>();
        for (String link:links.split("[;]")){
            logger.info("links after split...");
            logger.info("link: " + link);
            Pattern p = Pattern.compile("(\\S+)[,](\\S+)");
            Matcher m = p.matcher(link);
            while (m.find()) {
                logger.info("nameAndTag: " + m.group(1));
                logger.info("weight: " + m.group(2));
                String n = m.group(1).split(":")[0];
                String newTag = newTags.get(n);
                newLinks.add(n + ":" + newTag + "," + m.group(2));
            }
        }
//        Arrays.asList(links.split("[;]")).forEach(link -> {
//            logger.info("links after split...");
//            logger.info("link: " + link);
//            Pattern p = Pattern.compile("(\\S+)[,](\\S+)");
//            Matcher m = p.matcher(link);
//            while (m.find()) {
//                logger.info("nameAndTag: " + m.group(1));
//                logger.info("weight: " + m.group(2));
//                String n = m.group(1).split(":")[0];
//                String newTag = newTags.get(n);
//                newLinks.add(n + ":" + newTag + "," + m.group(2));
//            }
//        });
        logger.info("==================================================");
        logger.info("newLinks: ");
        for (String s : newLinks) logger.info(s);
        logger.info("==================================================");

        // 拼接成新的链接
//        String v = newLinks.stream().reduce((link1, link2) -> link1 + ";" + link2).orElse("");
        StringBuilder v = new StringBuilder();
        for (int i = 0; i < newLinks.size(); i++) {
            v.append(newLinks.get(i));
            if (i!=newLinks.size()-1) v.append(";");
        }
        logger.info("拼接成新的链接 newLinks: " + v);

        String nameAndTag = name + ":" + newTags.get(name);
        context.write(new Text(nameAndTag), new Text(v.toString()));
    }
}
