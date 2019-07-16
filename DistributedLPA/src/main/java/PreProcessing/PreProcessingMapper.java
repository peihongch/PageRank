package PreProcessing;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PreProcessingMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Pattern pattern = Pattern.compile("(\\S+)\\s\\[(.+)\\]");
        Matcher matcher = pattern.matcher(value.toString());
        String name = "", link = "";
        while (matcher.find()) {
            name = matcher.group(1);
            link = matcher.group(2);
        }
        StringBuilder links = new StringBuilder();
        String[] linkList = link.split("\\|");
        for (int i = 0; i < linkList.length; i++) {
            String[] splits = linkList[i].split(",");
            links.append(splits[0]).append(":").append(splits[0]).append(",").append(splits[1]);
            if (i != linkList.length - 1) links.append(";");
        }
        context.write(new Text(name + ":" + name), new Text(links.toString()));
    }
}
