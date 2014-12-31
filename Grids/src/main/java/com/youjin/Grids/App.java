package com.youjin.Grids;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * Hello world!
 */
public class App extends Configured implements Tool {

    // 格子词典文件在Conf中的存放name关键字
    private static final String CONF_DICT_FILE = "grid_dict_file";


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new App(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: Grids <grid_dict_file> <input_dir> <output_dir>");
            return 2;
        }

        String grid_dict_file = args[0];
        Path input_dir = new Path(args[1]);
        Path output_dir = new Path(args[2]);

        Configuration conf = getConf();
        conf.set(CONF_DICT_FILE, grid_dict_file);

        String task_name = String.format("grids: output_dir, %s ", output_dir);

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

        FileInputFormat.addInputPath(job, input_dir);
        FileOutputFormat.setOutputPath(job, output_dir);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static enum ErrorCounter {
        COL_COUNT_ERROR,
        NOT_FOUND_LAC,
        CELL_LEN_ERROR,
        TIME_FORMAT_ERROR,
        CELL_FORMAT_ERROR,
        GRID_FORMAT_ERROR,
    }

    public static class MapperImplement extends Mapper<Object, Text, Text, Text> {

        private Text keyOut = new Text();
        private Text valueOut = new Text();

        private HashMap<String, String> hashMap = new HashMap<String, String>();

        // 初始化，从HDFS上获取格子词典文件，加载到hashMap中
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();

            FileSystem file = FileSystem.get(conf);
            Path hdfsPath = new Path(conf.get(CONF_DICT_FILE));
            FSDataInputStream inputStream = file.open(hdfsPath);

            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] split = line.split("\t");

                String lacs = split[0];
                String grid = split[1];

                hashMap.put(lacs, grid);
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //原始数据 11-01 00:00:28E >87973|e2014-11-01 00:01:44|18629243043|47872|10902
            //输出数据 18629243043  x   y   0
            String record = value.toString();

            String[] split = record.split("\\|");
            // 检查列数
            if (split.length != 9) {
                context.getCounter(ErrorCounter.COL_COUNT_ERROR).increment(1);
                System.err.printf("COL_COUNT_ERROR:line %s length %d\n", key.toString(), split.length);
                return;
            }

            String cell = split[2]; // 获取手机号码
            String datetime = split[1]; // 获取时间
            String lac = String.format("%s_%s", split[3], split[4]); // 基站LAC

            // 检查手机长度
            if(cell.length() != 11) {
                context.getCounter(ErrorCounter.CELL_LEN_ERROR).increment(1);
                System.err.printf("CELL_LEN_ERROR:line %s cell %s\n", key.toString(), cell);
                return;
            }

            // 检查电话号码数据类型
            long cellNum;
            try {
                cellNum = Long.parseLong(cell);
            }catch (Exception ex){
                ex.printStackTrace();
                context.getCounter(ErrorCounter.CELL_FORMAT_ERROR).increment(1);
                return;
            }

            // 从词典中查找格子x,y及基站ID
            String grid = hashMap.get(lac);
            if (grid == null) {
                context.getCounter(ErrorCounter.NOT_FOUND_LAC).increment(1);
                System.err.printf("NOT_FOUND_LAC:line %s  lac %s\n", key.toString(), lac);
                return;
            }

            int indexTime;
            try {
                // 2014-11-01 00:01:44 ==> {00,01,44} ==> h*2+m/30 ==> 0 (把一天分成48等份)
                String[] time = datetime.split(" ")[1].split(":");
                indexTime = Integer.parseInt(time[0]) * 2 + Integer.parseInt(time[1]) / 30;
            }
            catch (Exception ex){
                ex.printStackTrace();
                context.getCounter(ErrorCounter.TIME_FORMAT_ERROR).increment(1);
                return;
            }

            // 输出数据 18629243043  x   y   0(时间段索引),
            // 这种方法会对数据进行半小时数据去噪，半小时内多条记录会被合并成一条
            keyOut.set(String.format("%d_%s_%d", cellNum, grid, indexTime));
            context.write(keyOut, valueOut);
        }
    }

    private static class ReducerImplement extends Reducer<Text, Text, Text, Text> {

        Text keyOut = new Text();
        Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // tel_x_y_indexTime
            String[] splitKey = key.toString().split("_");
            keyOut.set(splitKey[0]);

            // x y indexTime
            valueOut.set(String.format("%s\t%s\t%s", splitKey[1], splitKey[2], splitKey[3]));

            // (tel) (x y indexTime)
            context.write(keyOut, valueOut);
        }
    }
}
