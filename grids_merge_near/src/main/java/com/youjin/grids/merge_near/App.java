package com.youjin.grids.merge_near;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import java.util.ArrayList;
import java.util.List;

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

        System.out.println("grids_merge_near is running...");
        Job job = Job.getInstance(conf, "grids_merge_near");

        job.setJarByClass(App.class);
        job.setMapperClass(SumOnlineMapper.class);
        job.setReducerClass(SumOnlineReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
        REDUCER_ERROR,
    }

    public static class SumOnlineMapper extends Mapper<Object, Text, Text, Text> {

        private Text keyText = new Text();
        private Text valueText = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // (tel) (x y times)
            String[] split = value.toString().split("\t");
            String cell = split[0];
            String x = split[1];
            String y = split[2];
            int times = Integer.parseInt(split[3]);

            // 检查列数
            if (split.length != 4) {
                context.getCounter(ErrorCounter.COL_COUNT_ERROR).increment(1);
                return;
            }

            // 输出格式示例：13201512991   2964    987         5
            keyText.set(cell);
            valueText.set(String.format("%s\t%s\t%s", x, y, times));
            context.write(keyText, valueText);
        }
    }

    private static class SumOnlineReducer extends Reducer<Text, Text, Text, Text> {

        private Text valueText = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int timesMax1 = 0;      // 记录最大行次数
            int x1 = 0;             // 记录最大行x坐标
            int y1 = 0;             // 记录最大行y坐标

            int timesMax2 = 0;      // 记录次大行次数
            int x2 = 0;             // 记录次大行x坐标
            int y2 = 0;             // 记录次大行y坐标

            for (Text val : values) {

                // 分词获取xy及times
                String[] words = val.toString().split("\t");
                int x = Integer.parseInt(words[0]);
                int y = Integer.parseInt(words[1]);
                int times = Integer.parseInt(words[2]);

                // 查找最大值
                if (times > timesMax1) {
                    timesMax2 = timesMax1;
                    x2 = x1;
                    y2 = y1;

                    timesMax1 = times;
                    x1 = x;
                    y1 = y;
                }else if(times > timesMax2)
                {
                    timesMax2 = times;
                    x2 = x;
                    y2 = y;
                }
            }

            // 确认迭代是否正常执行
            if(timesMax1 == 0){
                context.getCounter(ErrorCounter.REDUCER_ERROR).increment(1);
                return;
            }

            // 检查最大次数的两行数据是否在相邻区域，如果在就把次数叠加给最大行
            if (Math.abs(x1 - x2) <= 1 && Math.abs(y1 - y2) <= 1) {
                timesMax1 += timesMax2;
            }

            // 设置value
            valueText.set(String.format("%s\t%s\t%s", x1, y1, timesMax1));

            // 输出格式示例：(13201512991   2964(x)     987(y))     25(次数)
            context.write(key, valueText);
        }
    }
}
