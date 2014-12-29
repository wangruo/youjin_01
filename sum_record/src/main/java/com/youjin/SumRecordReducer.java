package com.youjin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class SumRecordReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {

    private Text kText = new Text();
    private Text vText = new Text();

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
    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        // Data Format
        // 13712341234*31101 111*3432*20140911-QQ.....

        long sumTimes = 0;
        long sumFlows = 0;
        long latestDate = 0;
        String[] strArray = null;
        while (values.hasNext()) {

            String value = values.next().toString();

//			System.out.println("value=" + value);

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
            Long date = Long.valueOf(valueArray[2]);
            if (latestDate < date) {
                latestDate = date;
            }
        }

        String[] newKey = key.toString().split("-");
        vText.set(latestDate + "\t" + newKey[1] + "\t" + sumTimes + "\t"
                + sumFlows + strArray[1].replace("-", "\t"));
        kText.set(newKey[0]);

        output.collect(kText, vText);
    }
}
