package com.youjin.count;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

    private static final String CONF_SEPARATOR = "record_separator";
    private static final String CONF_KEY_COL = "key_col";
    private static final String CONF_COL_NUM = "col_num";


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
        options.addOption("separator", true, "分隔符");
        options.addOption("key_num", true, "要作为键值的列号");
        options.addOption("col_count", true, "总列数");
        CommandLine commandLine = parser.parse(options, remainingArgs);
        if (commandLine.getOptions().length != 5) {
            System.out.println("Usage: count -input <输入目录,逗号分隔多目录> -output <一个输出目录>" +
                    "-separator <分隔符> -key_num <作为键值的列号,逗号分隔> -col_num <分割后的总列数>");
            return 2;
        }

        String input = commandLine.getOptionValue("input");
        String output = commandLine.getOptionValue("output");
        String separator = commandLine.getOptionValue("separator");
        String key_num = commandLine.getOptionValue("key_num");
        String col_count = commandLine.getOptionValue("col_count");

        if(input == null || output == null || separator == null || key_num == null || col_count == null){
            System.out.printf("参数错误：input:%s\toutput:%s\tseparator:%s\tkey_num:%s\tcol_count:%s\n",
                    input,output,separator,key_num,col_count);
            return 3;
        }

        // 设置定界符
        conf.set(CONF_SEPARATOR, separator);

        // 设置列数到conf中
        int column_num = Integer.parseInt(col_count);
        conf.setInt(CONF_COL_NUM, column_num);

        // 检查指定列作为键值的数值检查，如  1,2,3
        // 执行中如果发生异常，那就可以直接在这里退出了
        String[] col = key_num.split(",");
        for(int i = 0; i < col.length; i++) {
            if( Integer.parseInt(col[i]) > column_num) {
                System.out.println("键值指定错误，超出最大列数");
                return 4;
            }
        }
        // 没有问题就设置到conf中
        conf.set(CONF_KEY_COL, key_num);


        System.out.println("sum_online is running...");
        Job job = Job.getInstance(conf, "count");

        job.setJarByClass(App.class);
        job.setMapperClass(SumOnlineMapper.class);
        job.setCombinerClass(SumOnlineReducer.class);
        job.setReducerClass(SumOnlineReducer.class);
        job.setNumReduceTasks(20);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        String[] sub_dir = input.split(",");
        for(int i = 0; i < sub_dir.length; i++) {
            FileInputFormat.addInputPath(job, new Path(sub_dir[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static enum ErrorCounter {
        LINE_COL_NUM_ERROR,
    }

    public static class SumOnlineMapper extends Mapper<Object, Text, Text, LongWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String separator = context.getConfiguration().get(CONF_SEPARATOR);
            String[] key_col = context.getConfiguration().get(CONF_KEY_COL).split(",");
            int col_num = context.getConfiguration().getInt(CONF_COL_NUM, 0);

            String record = value.toString();
            String[] split = record.split(separator);
            if(split.length != col_num){
                context.getCounter(ErrorCounter.LINE_COL_NUM_ERROR).increment(1);
                return;
            }

            StringBuilder buffer = new StringBuilder();
            int index = Integer.parseInt(key_col[0]);
            buffer.append(split[index - 1]);
            for(int i = 1; i < key_col.length; i++) {
                buffer.append(",");
                index = Integer.parseInt(key_col[0]);
                buffer.append(split[index - 1]);
            }

            context.write(new Text(buffer.toString()), new LongWritable(1));
        }
    }

    private static class SumOnlineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long sum = 0;
            for(LongWritable val : values){
                sum += val.get();
            }

            context.write(key, new LongWritable(sum));
        }
    }
}
