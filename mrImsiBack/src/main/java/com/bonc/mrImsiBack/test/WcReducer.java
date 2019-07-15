package com.bonc.mrImsiBack.test;

import com.bonc.mrImsiBack.enums.LOG_PROCESSOR_COUNTER;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result =new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum =0;
        for (IntWritable val : values) {
            context.getCounter(LOG_PROCESSOR_COUNTER.WC_REDUCE_WORD).increment(1);
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
