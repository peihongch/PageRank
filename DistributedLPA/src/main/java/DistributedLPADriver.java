import CalculateNewLabel.CalculateNewLabelMapper;
import CalculateNewLabel.CalculateNewLabelReducer;
import UpdateLabel.UpdateLabelMapper;
import UpdateLabel.UpdateLabelReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class DistributedLPADriver {
    private static Logger logger = LoggerFactory.getLogger(DistributedLPADriver.class);

    public static void main(String[] args) {
        int iter = args.length>2? Integer.parseInt(args[2]) :10;
        for (int i = 0; i < iter; i++) {
            Configuration conf = new Configuration();
            String input = i == 0 ? args[0] : args[1] + "/DataUpdate" + (i-1);
            if (runCalculateNewLabel(conf, input, args[1] + "/DataCal" + i)) {
                try {
                    DistributedCache.addCacheFile(new URI(args[1] + "/DataCal" + i + "/part-r-00000"), conf);
                } catch (URISyntaxException e) {
                    logger.info("设置DistributedCache出错！");
                    e.printStackTrace();
                }
                boolean flag = runUpdateLabel(conf, input, args[1] + "/DataUpdate" + i);
                if (!flag) {
                    logger.info("Something wrong...");
                    break;
                }
            }
        }
    }

    private static boolean runCalculateNewLabel(Configuration conf, String input, String output) {
        Job job;
        try {
            job = Job.getInstance(conf);
            FileSystem fileSystem = FileSystem.get(conf);
            job.setJobName("LabelPropagation");

            job.setJarByClass(DistributedLPADriver.class);
            job.setMapperClass(CalculateNewLabelMapper.class);
            job.setReducerClass(CalculateNewLabelReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            conf.set("mapred.textoutputformat.ignoreseparator", "true");
            conf.set("mapred.textoutputformat.separator", "\t");

            return setInputAndOutput(input, output, job, fileSystem);
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static boolean runUpdateLabel(Configuration conf, String input, String output) {
        try {
            Job job = Job.getInstance(conf);
            FileSystem fileSystem = FileSystem.get(conf);
            job.setJobName("LabelPropagation");

            job.setJarByClass(DistributedLPADriver.class);
            job.setMapperClass(UpdateLabelMapper.class);
            job.setReducerClass(UpdateLabelReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            conf.set("mapreduce.output.textoutputformat.separator", "|");

            return setInputAndOutput(input, output, job, fileSystem);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private static boolean setInputAndOutput(String input, String output, Job job, FileSystem fileSystem)
            throws IOException, InterruptedException, ClassNotFoundException {
        Path inPath = new Path(input);
        FileInputFormat.addInputPath(job, inPath);

        Path outPath = new Path(output);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true);
    }
}
