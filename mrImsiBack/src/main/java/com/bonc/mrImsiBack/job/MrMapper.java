package com.bonc.mrImsiBack.job;

import com.bonc.decodeMrXdr.MrXdrDecode;
import com.bonc.decodeMrXdr.entity.LocatorCombinedKeyMr;
import com.bonc.mrImsiBack.enums.LOG_PROCESSOR_COUNTER;
import mrLocateV2.datasource.MrXml;
import mrLocateV2.enums.mr.MrFormat;
import mrLocateV2.mrdata.MrPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class MrMapper extends Mapper<LongWritable, Text, LocatorCombinedKeyMr, Text> {
    String groupTag = "mr";
    private String inputString = null;
    MrPoint mrPoint = new MrPoint();
    Text txtMrParams = new Text();

    MrXdrDecode manager = null;
    ArrayList<String> basicParamList = new ArrayList<>();
    MrXml mrXml = new MrXml();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();

        Class c = null;
        try {
            c = Class.forName(conf.get("mr_classname"));
            manager = (MrXdrDecode) c.newInstance();
            manager.init();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        inputString = value.toString();
        Object result = manager.getMr(inputString);
        if (result == null) {
            context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_MR_WRONG_RECORDS).increment(1);
            return;
        }
        if (result instanceof MrPoint) {
            mrPoint = (MrPoint) result;
            generateOutput();
            getbasicParamList(mrPoint);
            context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_MR_RECORDS).increment(1);
            context.write(manager.getGroupKey(basicParamList), txtMrParams);
        } else {
            mrPoint = new MrPoint();
            ArrayList<ArrayList<String>> mrParamLists = (ArrayList<ArrayList<String>>) result;
            mrXml.setBasicParamList(mrParamLists.get(0));
            mrXml.setScCellParamList(mrParamLists.get(1));
            mrXml.setNcCellEarfcnList(mrParamLists.get(2));
            mrXml.setNcCellPciList(mrParamLists.get(3));
            mrXml.setNcCellRsrpList(mrParamLists.get(4));
            boolean flag = mrXml.createMrPoint(mrPoint, MrFormat.ZTE);
            if (flag) {
                generateOutput();
                context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_MR_RECORDS).increment(1);
                context.write(manager.getGroupKey(mrParamLists.get(0)), txtMrParams);
            } else {
                context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_MR_WRONG_RECORDS).increment(1);
                return;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    /**
     * 生成value,包含"mr"标识+MrPoint内容的字符串
     */
    private void generateOutput() {
        //打上mr标签，便于在reducer侧识别
        txtMrParams.set(groupTag + "\t" + mrPoint.toString());
    }

    public void getbasicParamList(MrPoint mrPoint){
        basicParamList.clear();
        basicParamList.add(String.valueOf(mrPoint.getTimeStamp()));
        basicParamList.add(String.valueOf(mrPoint.getMmeUeS1Apid()));
        basicParamList.add(mrPoint.getEventType() == null ? "" : mrPoint.getEventType().toString());
        basicParamList.add(String.valueOf(mrPoint.getMmeCode()));
        basicParamList.add(String.valueOf(mrPoint.getScCell().getEci()));
        basicParamList.add(String.valueOf(mrPoint.getMmeGroupid()));
        basicParamList.add(mrPoint.getImsi() == null ? "" : mrPoint.getImsi());
    }
}
