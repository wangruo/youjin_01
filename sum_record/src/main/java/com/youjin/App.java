package com.youjin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        JobConf conf = new JobConf(App.class);
        conf.setJobName("SumRecord");
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: SumRecord <GenericOptions> <in1> <in2> <out>");
            System.exit(2);
        }

        System.out.println("Summation Record is running...");

        conf.setInt(DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY, 1);
        conf.setJarByClass(App.class);

        conf.setMapperClass(SumRecordMapper.class);
        conf.setReducerClass(SumRecordReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(conf, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(conf, new Path(otherArgs[2]));
        JobClient.runJob(conf);
    }
}
