package com.youjin.general_sum;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class App extends Configured implements Tool {

    private static final String CONF_KEY_FIELD = "conf_key_field";
    private static final String CONF_MERGE_FIELD = "conf_merge_field";
    private static final String CONF_FIELDS_COUNT = "conf_fields_count";


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new App(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, ParseException {

        CommandLineParser parser = new PosixParser();
        Options options = new Options();
        options.addOption("input", true, "输入目录");
        options.addOption("output", true, "输出目录");
        options.addOption("key_field", true, "作为关键字的列");
        options.addOption("merge_field", true, "要迭代合并的列");
        options.addOption("fields_count", true, "总列数");

        CommandLine commandLine = parser.parse(options, args);
        if (commandLine.getOptions().length != 5) {
            System.out.println("Usage: general_sum -input <输入目录,逗号分隔多目录> -output <一个输出目录>" +
                    "-key_field <作为key的列号> -merge_field <要迭代合并的列号> -fields_count <总列数>");
            System.out.println("Example: hadoop jar general_sum-x.x.jar -input /data/category/internet_union/raws/internetlabel/2014/12/14" +
                    " -output /tmp/2014/12/14 -key_field 0,3 -merge_field 6,7 -fields_count 10");
            return 2;
        }

        String input = commandLine.getOptionValue("input");
        String output = commandLine.getOptionValue("output");
        String key_field = commandLine.getOptionValue("key_field");
        String merge_field = commandLine.getOptionValue("merge_field");
        String fields_count = commandLine.getOptionValue("fields_count");
        if (input == null || output == null || key_field == null || merge_field == null || fields_count == null) {
            System.out.printf("参数错误：input:%s\toutput:%s\tkey_field:%s\tmerge_field:%s\tfields_count:%s",
                    input, output, key_field, merge_field, fields_count);
            return 3;
        }

        Configuration conf = getConf();
        conf.set(CONF_KEY_FIELD, key_field);
        conf.set(CONF_MERGE_FIELD, merge_field);
        conf.set(CONF_FIELDS_COUNT, fields_count);

        String task_name = String.format("general_sum, output dir:%s ", output);
        System.out.printf("%s is running...", task_name);

        Job job = Job.getInstance(conf, task_name);

        job.setJarByClass(App.class);
        job.setMapperClass(MapperImplement.class);
        job.setReducerClass(ReducerImplement.class);

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
        FIELDS_COUNT_ERROR,
    }

    public static class MapperImplement extends Mapper<Object, Text, Text, Text> {

        private List<Integer> keys = new ArrayList<Integer>();
        private int fields_count;

        private Text keyOut = new Text();

        // 初始化，获取起始与终止时间
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            String[] keys_str = conf.get(CONF_KEY_FIELD).split(",");
            for (String key : keys_str) {
                keys.add(Integer.parseInt(key));
            }

            fields_count = conf.getInt(CONF_FIELDS_COUNT, -1);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split("\t");

            // 检查列数
            if (fields.length != fields_count) {
                context.getCounter(ErrorCounter.FIELDS_COUNT_ERROR).increment(1);
                return;
            }

            StringBuilder builder = new StringBuilder();
            // 生成key
            for (int id : keys) {
                builder.append(fields[id]);
                builder.append(",");
            }
            keyOut.set(builder.toString());

            // value 不修改直接输出
            context.write(keyOut, value);
        }
    }

    private static class ReducerImplement extends Reducer<Text, Text, Text, Text> {

        private List<Integer> fields = new ArrayList<Integer>();

        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            String[] fields_str = conf.get(CONF_MERGE_FIELD).split(",");
            for (String field : fields_str) {
                fields.add(Integer.parseInt(field));
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // 初始化求和变量组
            List<Long> sums = new ArrayList<Long>();
            for (int id : fields) {
                sums.add(0L);
            }

            String[] fields_str = null;
            // 迭代求和
            for (Text val : values) {
                fields_str = val.toString().split("\t");
                int i = 0;
                for (int id : fields) {
                    sums.set(i, sums.get(i) + Long.parseLong(fields_str[id]));
                    i++;
                }
            }

            // 检查分隔后的数据
            if(fields_str == null)
                return;

            // 替换合并后的字段
            int i = 0;
            for (int id : fields) {
                fields_str[id] = sums.get(i).toString();
                i++;
            }

            // 添加"\t"
            StringBuilder builder = new StringBuilder();
            boolean first = true;
            for(String field : fields_str){
                // 绕过key
                if(first) {
                    first =false;
                    continue;
                }

                builder.append(field);
                builder.append("\t");
            }

            // 设置key value
            keyOut.set(fields_str[0]);
            valueOut.set(builder.toString().trim());

            context.write(keyOut, valueOut);
        }
    }
}
