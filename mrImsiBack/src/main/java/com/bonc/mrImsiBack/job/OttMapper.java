package com.bonc.mrImsiBack.job;

import com.bonc.decodeMrXdr.MrOttDecode;
import com.bonc.decodeMrXdr.entity.LocatorCombinedKeyMr;
import com.bonc.decodeMrXdr.entity.OttDecode;
import com.bonc.mrImsiBack.enums.LOG_PROCESSOR_COUNTER;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OttMapper extends Mapper<LongWritable, Text, LocatorCombinedKeyMr, Text> {
    private static final Logger LOG = 	 LoggerFactory.getLogger(OttMapper.class);
    //从文件读出的字符串
    String inputString = null;
    MrOttDecode manager = null;
    String groupTag = "ott";
    //Value
    Text txtMmeXdrParams = new Text();
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration cfg = context.getConfiguration();
        String strClass = (String)cfg.get("ott_classname");
        if (strClass==null || strClass.equals("")) {
            LOG.info("OttMapper Error,none ottclassname found!");
            return;
        }
        try {
            Class onwClass = Class.forName(strClass);
            manager = (MrOttDecode)onwClass.newInstance();
            manager.init(cfg);
            LOG.info("OttDeocde Init sucess!");
        } catch (Exception e) {
            LOG.info("OttDeocde Exception:" + e.getMessage());
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        inputString = value.toString();
        OttDecode xdr = manager.getOtt(inputString);
        if (xdr != null) {
            if (xdr.getLon()!=0.0d && xdr.getLat()!=0.0d) {
                //生成value
                generateOutput(xdr);
                //map输出
                try {
                    LocatorCombinedKeyMr keyOut = manager.getGroupKey(xdr);
                    context.write(manager.getGroupKey(xdr), txtMmeXdrParams);
                } catch (Exception e) {
                    context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_OTT_WRONG_RECORDS).increment(1);
                    return;
                }
            } else {
                context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_OTT_WRONG_RECORDS).increment(1);
                return;
            }
        } else {
            context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_OTT_WRONG_RECORDS).increment(1);
            return;
        }
    }

    /**
     * 生成value,包含MmeXdr内容的字符串
     */
    protected void generateOutput(OttDecode xdr) {
        //打上"mmeXdr"标签，便于reducer侧识别
        txtMmeXdrParams.set(groupTag+"\t"+ manager.toString(xdr));
    }
}
