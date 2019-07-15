package com.bonc.mrImsiBack.job;

import com.bonc.decodeMrXdr.entity.LocatorCombinedKeyMr;
import mrLocateV2.bsparam.Cell;
import mrLocateV2.mrdata.MrPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

public class MrCombiner extends Reducer<LocatorCombinedKeyMr, Text, LocatorCombinedKeyMr, Text> {
    String[] tagedStrParams = new String[2];
    String groupTag = null;
    String strParam = null;
    Text txtMrParams = new Text();

    boolean isMergeNCell = false;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        isMergeNCell = conf.getBoolean("isCombine", false);
    }

    @Override
    protected void reduce(LocatorCombinedKeyMr key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        MrPoint mrPointModify = null;
        MrPoint mrPointTmp = null;

        for (Text value : values) {
            // 提取groupTag
            deTag(value);
            if (groupTag.equals("mmeXdr")) {
                context.write(key, value);
            } else {
                MrPoint mrPoint = null;
                // 转化为MrPoint对象 合并邻区
                if (isMergeNCell) {
                    // 每次来一条记录赋给mrPointTmp
                    mrPointTmp = new MrPoint();
                    mrPointTmp.stringTo(strParam);
                    // 初次加载mrPointModify
                    if (mrPointModify == null) {
                        mrPointModify = new MrPoint();
                        mrPointModify.stringTo(strParam);
                        continue;
                    }
                    // 合并邻区
                    if (mergeNCell(mrPointModify, mrPointTmp)) {
                        // 追加邻区
                        mrPointModify.getNcCellList().addAll((List<Cell>)mrPointTmp.getNcCellList().clone());
                        mrPointModify.getNcRsrpList().addAll((List<Double>)mrPointTmp.getNcRsrpList().clone());
                        continue;
                    } else {
                        mrPoint = (MrPoint)mrPointModify.clone();
                        mrPointModify = new MrPoint();
                        mrPointModify.stringTo(strParam);
                    }
                } else {
                    mrPoint = new MrPoint();
                    mrPoint.stringTo(strParam);
                }
                generateOutput(mrPoint);
                context.write(key, txtMrParams);
            }
        }

        if (mrPointModify != null && mrPointModify.getScCell() != null) {
            generateOutput(mrPointModify);
            context.write(key, txtMrParams);
        }
    }

    private void deTag(Text value) {
        String strValue = value.toString();
        tagedStrParams = strValue.split("\t");
        groupTag = tagedStrParams[0];
        strParam = tagedStrParams[1];
    }

    private boolean mergeNCell(MrPoint mrPointModify, MrPoint mrPointTmp){
        return (mrPointModify.getTimeStamp() == mrPointTmp.getTimeStamp()
                && mrPointModify.getScCell().getEci() == mrPointTmp.getScCell().getEci()
                && mrPointModify.getMmeUeS1Apid() == mrPointTmp.getMmeUeS1Apid()
                && mrPointModify.getMmeGroupid() == mrPointTmp.getMmeGroupid()
                && mrPointModify.getMmeCode() == mrPointTmp.getMmeCode());
    }

    /**
     * 生成value,包含"mr"标识+MrPoint内容的字符串
     */
    private void generateOutput(MrPoint mrPoint) {
        //打上mr标签，便于在reducer侧识别
        txtMrParams.set(groupTag + "\t" + mrPoint.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
