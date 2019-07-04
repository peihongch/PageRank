import LPA.LabelPropagationMapper;
import LPA.LabelPropagationReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class LabelPropagationDriver {
    public static void main(String[] args) {
        String input = args[0];
        String output = args[1];
        try {
            for (int i = 0; i < 3; i++) {
                if (i != 0) {
                    input = args[1] + "/Data" + i;
                }
                if (i != 2)
                    output = args[1] + "/Data" + (i + 1);
                else
                    output = args[1] + "/DataFinal";

                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf);
                FileSystem fileSystem = FileSystem.get(conf);
                job.setJobName("LabelPropagation");

                job.setJarByClass(LabelPropagationDriver.class);
                job.setMapperClass(LabelPropagationMapper.class);
                job.setReducerClass(LabelPropagationReducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                conf.set("mapred.textoutputformat.ignoreseparator", "true");
                conf.set("mapred.textoutputformat.separator", "|");

                Path inPath = new Path(input);
                FileInputFormat.addInputPath(job, inPath);

                Path outPath = new Path(output);
                if (fileSystem.exists(outPath)) {
                    fileSystem.delete(outPath, true);
                }
                FileOutputFormat.setOutputPath(job, outPath);

                job.waitForCompletion(true);
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
