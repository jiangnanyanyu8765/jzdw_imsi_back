package com.bonc.mrImsiBack.job;

import com.bonc.decodeMrXdr.MrXdrDecode;
import com.bonc.decodeMrXdr.entity.LocatorCombinedKeyMr;
import com.bonc.decodeMrXdr.entity.S1MMEXdr;
import com.bonc.mrImsiBack.enums.LOG_PROCESSOR_COUNTER;
import com.bonc.mrImsiBack.utils.Imsi2MrJoins;
import mrLocateV2.bsparam.Cell;
import mrLocateV2.mrdata.MrPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ImsiBackUpReducer extends Reducer<LocatorCombinedKeyMr, Text, NullWritable, Text> {
    NullWritable outkey = NullWritable.get();
    Text txtMr = new Text();

    // MrPoint对象初始化
    S1MMEXdr[] xdrArr = new S1MMEXdr[2];
    List<MrPoint> mrPointList = new ArrayList<MrPoint>();
    int xdr2MrTimeOffset = 0;
    // mrPointList是否形成判断
    boolean isComplete = false;
    // 是否匹配到imsi
    boolean isImsiMatched = false;
    int mrInterval = 30;
    int xdr2MrInterval = 30;
    int mrMaxSize = 150;

    // groupTag+Params
    String[] tagedStrParams = new String[2];
    String groupTag = null;
    String strParam = null;

    // MmeXdr对象初始化 Joins对象初始化
    Imsi2MrJoins imsi2MrJoins = new Imsi2MrJoins();
    MrXdrDecode manager = null;

    //是否合并邻区
    boolean isMergeNCell = false;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        isMergeNCell = conf.getBoolean("mr_needCombine", false);
        xdr2MrTimeOffset = conf.getInt("xdr2MrTimeOffset", 0);
        imsi2MrJoins.setXdr2MrTimeOffset(xdr2MrTimeOffset);
        xdr2MrInterval = conf.getInt("xdr2MrInterval", 30);
        imsi2MrJoins.setXdr2MrInterVal(xdr2MrInterval);
        mrMaxSize = conf.getInt("mrMaxSize", 150);
        mrInterval = conf.getInt("mrInterval", 30);

        String strClass = conf.get("xdr_classname");
        if (strClass==null || strClass.equals("")) {
            return;
        }
        try {
            Class onwClass = Class.forName(strClass);
            manager = (MrXdrDecode) onwClass.newInstance();
            manager.init();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            return;
        }
    }

    @Override
    protected void reduce(LocatorCombinedKeyMr key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 清空
//        mmeXdrMap.clear();
//        mmeXdrList.clear();
        xdrArr[0] = null;
        xdrArr[1] = null;
        mrPointList.clear();
        // 合并后的mr，当合并结束后赋给mrPoint，并重新变为null
        MrPoint mrPointModify = null;
        // 每次来一条记录赋给mrPointTmp
        MrPoint mrPointTmp = null;

        for (Text value : values) {
//            context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_DATA_RECORDS).increment(1);
            deTag(value);
            if (groupTag.equals("mmeXdr")) {
                S1MMEXdr mmeXdr = new S1MMEXdr();
                manager.stringTo(strParam,mmeXdr);
                // 保存到mmeXdrMap中,先判断mmeXdr合法
                if (mmeXdr.getStartTime() != -1 && mmeXdr.getEndTime() != -1 && mmeXdr.getImsi() != null && !"\\N".equals(mmeXdr.getImsi())) {
                    context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_XDR_RECORDS).increment(1);
//                    mmeXdrMap.put(mmeXdr.getStartTime(), mmeXdr);
//                    mmeXdrList.add(mmeXdr);
                    if (xdrArr[0] == null) {
                        xdrArr[0] = mmeXdr;
                        xdrArr[1] = null;
                    } else if (xdrArr[1] == null) {
                        xdrArr[1] = mmeXdr;
                    } else {
                        xdrArr[0] = xdrArr[1];
                        xdrArr[1] = mmeXdr;
                    }
                    if (xdrArr[0] != null && xdrArr[1] != null && !mrPointList.isEmpty()) {
                        // TODO mr xdr imsi回填
                        imsi2MrJoins.attachImsiToMrPointList(mrPointList, xdrArr, context);
                        // TODO 输出
                        outputMrPointList(context, mrPointList);
                        // 并启动下一个List
                        mrPointList.clear();
                    }
                }
            }
            if (groupTag.equals("mr")) {
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
                        context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_MR_COMBINE_RECORDS).increment(1);
//                        mrPointModify = new MrPoint();
                        mrPointModify.reset();
                        mrPointModify.stringTo(strParam);
                    }
                } else {
//                    System.out.println(strParam);
                    mrPoint = new MrPoint();
                    mrPoint.stringTo(strParam);
                    context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_MR_COMBINE_RECORDS).increment(1);
                }
                mrPointList.add(mrPoint);
            }
        }

        //在循环体之外
        if (!mrPointList.isEmpty()) {
            if (xdrArr[0] != null) {
                // TODO mr xdr imsi回填
                imsi2MrJoins.attachImsiToMrPointList(mrPointList, xdrArr, context);
            } else {
                context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_imsiNoMatchCount).increment(mrPointList.size());
            }
            // TODO 输出
            outputMrPointList(context, mrPointList);
            // 并启动下一个List
            mrPointList.clear();
        }
        if (mrPointModify != null && mrPointModify.getScCell() != null) {
            context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_MR_COMBINE_RECORDS).increment(1);
            mrPointList.add(mrPointModify);
            if (xdrArr[0] != null) {
                // TODO mr xdr imsi回填
                imsi2MrJoins.attachImsiToMrPointList(mrPointList, xdrArr, context);
            } else {
                context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_imsiNoMatchCount).increment(mrPointList.size());
            }
            // TODO 输出
            outputMrPointList(context, mrPointList);
            // 并启动下一个List
            mrPointList.clear();
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

    private boolean generateMrPointList(MrPoint mrPoint, List<MrPoint> mrPointList, long timeInterval, int maxSize) {
        // 指示一个完整通话list是否完成，初始化为未完成。
        boolean isComplete = false;
        if (mrPointList.isEmpty()) {// 如果为空，直接加入
//			if (!mrPoint.getNcCellList().isEmpty()) {
//				mrPointList.add(mrPoint);
//			}
            mrPointList.add(mrPoint);
            isComplete = false;
        } else {// 如果不为空，中根据实际条件追加
            MrPoint lastPoint = mrPointList.get(mrPointList.size() - 1);
            long mrPointTime = mrPoint.getTimeStamp();
            long lastPointTime = lastPoint.getTimeStamp();
            if ((mrPointTime - lastPointTime) <= timeInterval * 1000 && mrPointList.size() < maxSize) {
//				if (!mrPoint.getNcCellList().isEmpty()) {
//					mrPointList.add(mrPoint);
//				}
                mrPointList.add(mrPoint);
                // 返回false，说明还未装满
                isComplete = false;
            } else {// 间隔超过30s或者大于maxSize
                // 返回true，说明已经装满
                isComplete = true;
            }
        }
        // 返回List状态
        return isComplete;
    }

    private void outputMrPointList(Context context, List<MrPoint> mrPointList) {
        for (MrPoint mrPoint : mrPointList) {
            try {
                if (mrPoint.getImsi() != null && !"".equals(mrPoint.getImsi())) {
                    txtMr.set(mrPoint.toString());
                    context.write(outkey, txtMr);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        mrPointList.clear();
//        mmeXdrMap.clear();
//        mmeXdrList.clear();
    }
}
