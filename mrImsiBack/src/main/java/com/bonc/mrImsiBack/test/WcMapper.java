package com.bonc.mrImsiBack.test;

import com.bonc.mrImsiBack.enums.LOG_PROCESSOR_COUNTER;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one =new IntWritable(1);
    private Text word =new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr =new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.getCounter(LOG_PROCESSOR_COUNTER.WC_MAP_WORD).increment(1);
            context.write(word, one);
        }
    }
}
