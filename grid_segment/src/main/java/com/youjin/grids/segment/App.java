package com.youjin.grids.segment;

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

    private static final String CONF_TIME_BEGIN_1 = "conf_time_begin_1";
    private static final String CONF_TIME_END_1 = "conf_time_end_1";

    private static final String CONF_TIME_BEGIN_2 = "conf_time_begin_2";
    private static final String CONF_TIME_END_2 = "conf_time_end_2";


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
        options.addOption("time1", true, "时间范围1");
        options.addOption("time2", true, "时间范围2");
        CommandLine commandLine = parser.parse(options, remainingArgs);
        if (commandLine.getOptions().length < 3 || commandLine.getOptions().length > 4) {
            System.out.println("Usage: segment -input <输入目录,逗号分隔多目录> -output <一个输出目录>" +
                    "-time1 <时间范围（0,47，半小时计数,逗号分隔起始）> -time2 <同time>");
            return 2;
        }

        String input = commandLine.getOptionValue("input");
        String output = commandLine.getOptionValue("output");
        String time1 = commandLine.getOptionValue("time1");
        String time2 = commandLine.getOptionValue("time2");

        if (input == null || output == null || (time1 == null && time2 == null)) {
            System.out.printf("参数错误：input:%s\toutput:%s\ttime1:%s\ttime2:%s", input, output, time1, time2);
            return 3;
        }

        if (time1 != null) {
            // 执行中如果发生异常，那就可以直接在这里退出了
            String[] col = time1.split(",");
            int time_start1 = Integer.parseInt(col[0]);
            int time_end1 = Integer.parseInt(col[1]);
            if (time_start1 > time_end1) {
                System.out.println("参数错误：起始时间大于结束时间！");
                return 4;
            }

            // 设置时间范围
            conf.setInt(CONF_TIME_BEGIN_1, time_start1);
            conf.setInt(CONF_TIME_END_1, time_end1);
        }

        // 测试time2
        if (time2 != null) {
            String[] col = time2.split(",");
            int time_start2 = Integer.parseInt(col[0]);
            int time_end2 = Integer.parseInt(col[1]);
            if (time_start2 > time_end2) {
                System.out.println("参数错误：起始时间大于结束时间！");
                return 4;
            }

            // 设置时间范围
            conf.setInt(CONF_TIME_BEGIN_2, time_start2);
            conf.setInt(CONF_TIME_END_2, time_end2);
        }

        String task_name = String.format("grids_segment:%s ", output);
        System.out.printf("%s is running...", task_name);

        Job job = Job.getInstance(conf, task_name);

        job.setJarByClass(App.class);
        job.setMapperClass(SumOnlineMapper.class);
        job.setReducerClass(SumOnlineReducer.class);
        job.setNumReduceTasks(10);

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
        CELL_LENGTH_ERROR,
    }

    public static class SumOnlineMapper extends Mapper<Object, Text, Text, IntWritable> {

        private int start_time1 = 0;
        private int end_time1 = 0;
        private int start_time2 = 0;
        private int end_time2 = 0;
        private Text keyOut = new Text();
        private final IntWritable valueOut = new IntWritable(1);

        // 初始化，获取起始与终止时间
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            start_time1 = context.getConfiguration().getInt(CONF_TIME_BEGIN_1, -1);
            end_time1 = context.getConfiguration().getInt(CONF_TIME_END_1, -1);
            start_time2 = context.getConfiguration().getInt(CONF_TIME_BEGIN_2, -1);
            end_time2 = context.getConfiguration().getInt(CONF_TIME_END_2, -1);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // (tel) (x y indexTime)
            String[] split = value.toString().split("\t");

            // 检查列数
            if (split.length != 4) {
                context.getCounter(ErrorCounter.COL_COUNT_ERROR).increment(1);
                return;
            }

            String cell = split[0];
            String x = split[1];
            String y = split[2];
            String indexTime = split[3];

            // 检查电话号码长度
            if (cell.length() != 11) {
                context.getCounter(ErrorCounter.CELL_LENGTH_ERROR).increment(1);
                return;
            }

            // 检查是否需要该时间段数据
            int time = Integer.parseInt(indexTime);
            if ((time >= start_time1 && time <= end_time1) || (time >= start_time2 && time <= end_time2)) {
                // 输出格式示例：13201512991_2964_987         1
                keyOut.set(String.format("%s_%s_%s", cell, x, y));
                context.write(keyOut, valueOut);
            }
        }
    }

    private static class SumOnlineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Text keyOut = new Text();
        private IntWritable valueOut = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // 13201512991_2964_987         1
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            keyOut.set(key.toString().replace("_", "\t"));
            valueOut.set(sum);

            // 输出格式示例：(13201512991 2964(x) 987(y))     25(次数)
            context.write(keyOut, valueOut);
        }
    }
}
