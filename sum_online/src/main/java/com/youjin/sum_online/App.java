package com.youjin.sum_online;

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

/**
 * Hello world!
 */
public class App extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new App(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length != 0) {
            System.out.println("Usage: sum_online");
            return 2;
        }

        System.out.println("sum_online is running...");

        Job job = Job.getInstance(conf, "sum_online");

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

//        FileInputFormat.addInputPath(job, new Path("E:/test/grids/loc/a"));
//        FileInputFormat.addInputPath(job, new Path("E:/test/grids/loc/b"));
//        FileOutputFormat.setOutputPath(job, new Path("E:/test/grids/output"));

//        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/09"));
//        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/10"));
//        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/11"));
//        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/13"));
//        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/15"));
//        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/16"));
//        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/17"));
//        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/20"));
//        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/21"));
//        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/22"));
//        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/23"));
//        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/24"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/25"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/26"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/27"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/28"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/29"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/30"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/10/31"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/11/01"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/11/02"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/11/03"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/11/04"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/11/05"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/11/06"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/11/07"));
        FileInputFormat.addInputPath(job, new Path("/data/signal/periodicupdate/2014/11/08"));
        FileOutputFormat.setOutputPath(job, new Path("/data/category/location_info/sum_online"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static enum ErrorCounter {
        MAP_FILE_ERROR,
        LOC_FILE_ERROR,
        TEL_LEN_ERROR,
    }

    public static class SumOnlineMapper extends Mapper<Object, Text, Text, LongWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //原始数据 11-01 00:00:28E >87973|e2014-11-01 00:01:44|18629243043|47872|10902
            //输出数据 18629243043  2014-11-01 00:01:44  id   x   y
            String record = value.toString();

            String[] split = record.split("\\|");
            if(split.length != 5){
                context.getCounter(ErrorCounter.LOC_FILE_ERROR).increment(1);
                System.out.printf("LOC_FILE_ERROR:line %s length %d\n", split[1], split.length);
                return;
            }
            String tel = split[2];
            if(tel.length() != 11) {
                context.getCounter(ErrorCounter.TEL_LEN_ERROR).increment(1);
                return;
            }

            context.write(new Text(tel), new LongWritable(1));
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
