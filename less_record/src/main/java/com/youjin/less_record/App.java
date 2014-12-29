package com.youjin.less_record;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
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
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class App 
{
    private static Logger logger = Logger.getLogger(App.class);

    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: LessRecord <GenericOptions> <in1> <in2> <out> <sec_local>");
            System.exit(2);
        }
        conf.set("sec_local_file", otherArgs[3]);
        System.out.println("LessRecord is running...");

        conf.setInt(DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY, 1);
        Job job = Job.getInstance(conf);

        job.setJobName("LessRecord");
        job.setJarByClass(App.class);

        job.setMapperClass(LessRecordMapper.class);
        job.setReducerClass(LessRecordReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    static class LessRecordMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text kText = new Text();
        private Text vText = new Text();

        private String[] recordArray = null;

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String record = value.toString();
            recordArray = record.split("\t");

            StringBuffer buffer = new StringBuffer();
            if (recordArray.length > 5) {
                for (int i = 5; i < recordArray.length; i++) {
                    buffer.append("-" + recordArray[i]);
                }
            } else if (recordArray.length < 5) {
                return;
            }

            String k = recordArray[0] + "-" + recordArray[2];
            String v = recordArray[3] + "-" + recordArray[4] + "-"
                    + recordArray[1] + "_" + buffer.toString();

            kText.set(k);
            vText.set(v);

            context.write(kText, vText);
        }
    }

    public static class LessRecordReducer extends Reducer<Text, Text, Text, Text> {

        private static Text kText = new Text();
        private static Text vText = new Text();
        private static SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        private static Map<String, String> map = null;

        private static Map<String, String> LoadLocationFile(Configuration conf) {
            HashMap<String, String> map = new HashMap();

            try {
                FileSystem file = FileSystem.get(conf);
                Path hdfsPath = new Path(conf.get("sec_local_file"));
                FSDataInputStream inputStream = file.open(hdfsPath);

                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line;
                while((line = reader.readLine()) != null){
                    String[] str = line.split("\t");
                    map.put(str[3], str[1]);
                }
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }

            return map;
        }

        private static String GetLocation(Map<String, String> map, String key)
        {
            String value=map.get(key);

            if(value != null)
                return value;

            return "未知";
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            if (map == null) {
                map = LoadLocationFile(context.getConfiguration());
            }

            long sumTimes = 0;
            long sumFlows = 0;
            long latestDate = 0;
            String[] strArray = null;

            for(Text text: values) {

                String value = text.toString();

                // split 111*3432*20140911-QQ.....
                strArray = value.split("_");
                if (strArray == null || strArray.length != 2)
                    continue;

                // split 111*3432*20140911
                String[] valueArray = strArray[0].split("-");
                if (valueArray == null || valueArray.length != 3)
                    continue;

                sumTimes += Long.valueOf(valueArray[0]);
                sumFlows += Long.valueOf(valueArray[1]);
                long date = Long.valueOf(valueArray[2]);
                if (latestDate < date) {
                    latestDate = date;
                }
            }

            Date latest = null;
            long diff_day = 0;
            try {
                latest = format.parse(String.valueOf(latestDate));
                long diff = (new Date()).getTime() - latest.getTime();
                diff_day = diff / (1000 * 60 * 60 * 24);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            String[] newKey = key.toString().split("-");
            kText.set(newKey[0]);

            String location_key = newKey[0].substring(0, 7);
            String location = GetLocation(map, location_key);
            vText.set(latestDate + "\t" + newKey[1] + "\t" + sumTimes + "\t"
                    + sumFlows + "\t" + diff_day + strArray[1].replace("-", "\t")+ "\t" + location);

            context.write(kText, vText);
        }
    }
}
