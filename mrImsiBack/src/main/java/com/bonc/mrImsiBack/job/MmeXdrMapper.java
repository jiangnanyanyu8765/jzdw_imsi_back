package com.bonc.mrImsiBack.job;

import com.bonc.decodeMrXdr.MrXdrDecode;
import com.bonc.decodeMrXdr.entity.LocatorCombinedKeyMr;
import com.bonc.decodeMrXdr.entity.S1MMEXdr;
import com.bonc.mrImsiBack.enums.LOG_PROCESSOR_COUNTER;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MmeXdrMapper extends Mapper<LongWritable, Text, LocatorCombinedKeyMr, Text> {
    String groupTag = "mmeXdr";
    Text txtMmeXdrParams = new Text();
    String inputString = null;
    S1MMEXdr xdr = null;
    long XDR_START_TIME = 0;
    long XDR_END_TIME = 0;
    MrXdrDecode manager = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        XDR_START_TIME = conf.getLong("xdr_start_time", 0);
        XDR_END_TIME = conf.getLong("xdr_end_time", 0);

        Class c = null;
        try {
            c = Class.forName(conf.get("xdr_classname"));
            manager = (MrXdrDecode) c.newInstance();
            manager.init();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        inputString = value.toString();
        xdr = manager.getXdr(inputString);
        if (xdr != null) {
            if (XDR_START_TIME != 0 && XDR_END_TIME != 0) {
                if (xdr.getStartTime() >= XDR_START_TIME && xdr.getStartTime() <= XDR_END_TIME) {
                    generateOutput(xdr);
                    context.write(manager.getGroupKey(xdr), txtMmeXdrParams);
                }
            } else {
                generateOutput(xdr);
                context.write(manager.getGroupKey(xdr), txtMmeXdrParams);
            }
        } else {
            context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_XDR_WRONG_RECORDS).increment(1);
            return;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    /**
     * 生成value,包含MmeXdr内容的字符串
     */
    protected void generateOutput(S1MMEXdr xdr) {
        // 打上"mmeXdr"标签，便于reducer侧识别
        txtMmeXdrParams.set(groupTag + "\t" + manager.toString(xdr));
    }
}
