package com.youjin.grids.segment.merge;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class App extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new App(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, ParseException {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        CommandLineParser parser = new PosixParser();
        Options options = new Options();
        options.addOption("input", true, "输入目录");
        options.addOption("output", true, "输出目录");
        CommandLine commandLine = parser.parse(options, remainingArgs);
        if (commandLine.getOptions().length != 2) {
            System.out.println("Usage: segment -input <输入目录,逗号分隔多目录> -output <一个输出目录>");
            return 2;
        }

        String input = commandLine.getOptionValue("input");
        String output = commandLine.getOptionValue("output");

        if (input == null || output == null) {
            System.out.printf("参数错误：input:%s\toutput:%s", input, output);
            return 3;
        }

        System.out.println("grids_segment_merge is running...");
        Job job = Job.getInstance(conf, "grids_segment_merge");

        job.setJarByClass(App.class);
        job.setMapperClass(SumOnlineMapper.class);
        job.setReducerClass(SumOnlineReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        String[] sub_dir = input.split(",");
        for (String sub : sub_dir) {
            FileInputFormat.addInputPath(job, new Path(sub));
        }
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static enum ErrorCounter {
        COL_COUNT_ERROR,
    }

    public static class SumOnlineMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text keyText = new Text();
        private IntWritable valueInt = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // tel_x_y times
            String[] split = value.toString().split("\t");

            // 检查列数
            if (split.length != 2) {
                context.getCounter(ErrorCounter.COL_COUNT_ERROR).increment(1);
                return;
            }

            String keyString = split[0];
            int times = Integer.parseInt(split[1]);

            // 输出格式示例：13201512991_2964_987         5
            keyText.set(keyString);
            valueInt.set(times);
            context.write(keyText, valueInt);
        }
    }

    private static class SumOnlineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Text keyText = new Text();
        private IntWritable valueInt = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            keyText.set(key.toString().replace("_", "\t"));
            valueInt.set(sum);

            // 输出格式示例：(13201512991   2964(x)     987(y))     25(次数)
            context.write(keyText, valueInt);
        }
    }
}
