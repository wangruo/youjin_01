package com.youjin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class SumRecordMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, Text> {

    //    private static final Log LOG = LogFactory.getLog(SumRecordMapper.class.getName());
    private Text kText = new Text();
    private Text vText = new Text();

    private String[] recordArray = null;

    /**
     * Default implementation that does nothing.
     */
    public void close() throws IOException {
    }

    /**
     * Default implementation that does nothing.
     */
    public void configure(JobConf job) {

    }

    @Override
    public void map(LongWritable key, Text value,
                    OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        String record = value.toString();
        //LOG.info("wr0 record=" + record);

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

//		LOG.info("wr1 key=" + kText + "  value=" + vText);
//		System.out.println("wr2 key=" + kText + "  value=" + vText);

        output.collect(kText, vText);
    }
}