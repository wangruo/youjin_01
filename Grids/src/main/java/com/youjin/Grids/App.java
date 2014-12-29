package com.youjin.Grids;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
        int res = ToolRunner.run(new Configuration(), new App(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length != 3) {
            System.out.println("Usage: Grids <grid_dict_file> <input_dir> <output_dir>");
            return 2;
        }

        String grid_dict_file = remainingArgs[0];
        Path input_dir = new Path(remainingArgs[1]);
        Path output_dir = new Path(remainingArgs[2]);

        conf.set(CONF_DICT_FILE, grid_dict_file);

        String task_name = String.format("grids:%s ", input_dir);
        System.out.printf("%s is running...", task_name);

        Job job = Job.getInstance(conf, task_name);

        job.setJarByClass(App.class);

        job.setMapperClass(GridsMapper.class);
        job.setReducerClass(GridsReducer.class);
        job.setNumReduceTasks(10);

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
        DICT_FILE_ERROR,
        COL_COUNT_ERROR,
        NOT_FOUND_LAC,
        CELL_LEN_ERROR,
        TIME_FORMAT_ERROR,
        CELL_FORMAT_ERROR,
        GRID_FORMAT_ERROR,
    }

    public static class GridsMapper extends Mapper<Object, Text, Text, Text> {

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
                if (split.length != 7) {
                    context.getCounter(ErrorCounter.DICT_FILE_ERROR).increment(1);
                    System.out.printf("MAP_FILE_ERROR:line %s length %d\n", split[1], split.length);
                    continue;
                }

                String[] lac = split[6].split("/");
                for (String item : lac) {
                    String value = String.format("%s_%s_%s", split[1], split[4], split[5]);
                    hashMap.put(item, value);
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //原始数据 11-01 00:00:28E >87973|e2014-11-01 00:01:44|18629243043|47872|10902
            //输出数据 18629243043  x   y   0
            String record = value.toString();

            String[] split = record.split("\\|");
            // 检查列数
            if (split.length != 5) {
                context.getCounter(ErrorCounter.COL_COUNT_ERROR).increment(1);
                System.err.printf("COL_COUNT_ERROR:line %s length %d\n", key.toString(), split.length);
                return;
            }

            String cell = split[2]; // 获取手机号码
            String datetime = split[1].replace("e", ""); // 去掉时间前面的e
            String lac = split[3]; // 基站LAC
            String cellId = split[4]; // 基站CellID，扇区

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
            String baseStation = String.format("%s_%s", lac, cellId);
            String grid = hashMap.get(baseStation);
            if (grid == null) {
                context.getCounter(ErrorCounter.NOT_FOUND_LAC).increment(1);
                System.err.printf("NOT_FOUND_LAC:line %s  lac %s\n", key.toString(), baseStation);
                return;
            }

            // 基站数据库ID，坐标x，坐标y id_x_y ==> id x y
            int x;
            int y;
            try {
                String[] str = grid.split("_");
                x = Integer.parseInt(str[1]);
                y = Integer.parseInt(str[2]);
            }
            catch (Exception ex) {
                ex.printStackTrace();
                context.getCounter(ErrorCounter.GRID_FORMAT_ERROR).increment(1);
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

            //输出数据 18629243043  x   y   0(时间段索引)
            keyOut.set(String.format("%d_%d_%d_%d", cellNum, x, y, indexTime));
            context.write(keyOut, valueOut);
        }
    }

    private static class GridsReducer extends Reducer<Text, Text, Text, Text> {

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
