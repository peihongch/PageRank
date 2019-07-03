import GraphBuilder.GraphBuilderMapper;
import GraphBuilder.GraphBuilderReducer;
import PageRankIter.PagePankIterMapper;
import PageRankIter.PagePankIterReducer;
import RankViewer.RankViewerMapper;
import RankViewer.RankViewerReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.DecDoubleWritable;

import java.io.IOException;

public class PageRankDriver {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.setDouble("d", 0.85);
        configuration.setInt("N", 4);
        if (runGraphBuilder(configuration, args[0], args[1] + "/Data0")) {
            boolean flag = true;
            int i = 0;
            for (; i < 10; i++) {
                if (!runPageRankIter(configuration, args[1] + "/Data" + i, args[1] + "/Data" + (i + 1))) {
                    flag = false;
                }
            }
            if (flag) {
                runRankViewer(configuration, args[1] + "/Data" + i, args[1] + "/DataFinal");
            }
        }
    }

    private static boolean runGraphBuilder(Configuration configuration, String input, String output) {
        try {
            Job job = Job.getInstance(configuration);
            FileSystem fileSystem = FileSystem.get(configuration);
            job.setJobName("GraphBuilder");

            job.setJarByClass(PageRankDriver.class);
            job.setMapperClass(GraphBuilderMapper.class);
            job.setReducerClass(GraphBuilderReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);

            setInputAndOutput(input, output, job, fileSystem);

            return job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("runGraphBuilder出错！");
        } catch (InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static boolean runPageRankIter(Configuration configuration, String input, String output) {
        try {
            Job job = Job.getInstance(configuration);
            FileSystem fileSystem = FileSystem.get(configuration);
            job.setJobName("PageRankIter");

            job.setJarByClass(PageRankDriver.class);
            job.setMapperClass(PagePankIterMapper.class);
            job.setReducerClass(PagePankIterReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setInputFormatClass(KeyValueTextInputFormat.class);

            setInputAndOutput(input, output, job, fileSystem);

            return job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("runPageRankIter出错！");
        } catch (InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static boolean runRankViewer(Configuration configuration, String input, String output) {
        try {
            Job job = Job.getInstance(configuration);
            FileSystem fileSystem = FileSystem.get(configuration);
            job.setJobName("RankViewer");

            job.setJarByClass(PageRankDriver.class);
            job.setMapperClass(RankViewerMapper.class);
            job.setReducerClass(RankViewerReducer.class);
            job.setMapOutputKeyClass(DecDoubleWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setInputFormatClass(KeyValueTextInputFormat.class);

            setInputAndOutput(input, output, job, fileSystem);

            return job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("runRankViewer出错！");
        } catch (InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static void setInputAndOutput(String input, String output, Job job, FileSystem fileSystem) throws IOException {
        Path inPath = new Path(input);
        FileInputFormat.addInputPath(job, inPath);

        Path outPath = new Path(output);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
    }
}
