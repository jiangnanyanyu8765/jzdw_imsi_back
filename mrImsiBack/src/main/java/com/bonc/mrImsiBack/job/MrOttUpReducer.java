package com.bonc.mrImsiBack.job;

import cn.com.bonc.main.TransformCoordinate;
import cn.com.bonc.util.inter.BaseCoordinateTransInt;
import com.bonc.decodeMrXdr.MrOttDecode;
import com.bonc.decodeMrXdr.entity.LocatorCombinedKeyMr;
import com.bonc.decodeMrXdr.entity.OttDecode;
import com.bonc.mrImsiBack.enums.LOG_PROCESSOR_COUNTER;
import com.bonc.mrImsiBack.utils.Imsi2MrJoins;
import mrLocateV2.mrdata.MrPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MrOttUpReducer extends Reducer<LocatorCombinedKeyMr, Text, NullWritable, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(MrOttUpReducer.class);
    NullWritable outkey = NullWritable.get();
    Text txtMr = new Text();

    //MmeXdr对象初始化
//    OttDecode ott = new OttDecode();
    //Joins对象初始化
    Imsi2MrJoins imsi2MrJoins = new Imsi2MrJoins();

    String[] tagedStrParams = new String[2];
    String groupTag = null;
    String strParam = null;

    //MrPoint对象初始化
//    MrPoint mrPoint = new MrPoint();
    //mrPointList
    List<MrPoint> mrPointList = new ArrayList<MrPoint>();
//    Map<Long, OttDecode> ottMap = new HashMap<Long, OttDecode>();
//    List<OttDecode> ottList = new ArrayList();
    OttDecode[] xdrArr = new OttDecode[2];
    //xdr2mrTime xdr相对mr的时间偏差，提前为正，落后为负，默认为0
    int xdr2MrTimeOffset = 0;
    //mrPointList是否形成判断
    boolean isComplete = false;
    //是否匹配到imsi
    boolean isImsiMatched = false;
    boolean isDelete = false;
    //是否对mrPoint的定位结果平均化处理
//    boolean isAvg = false;
//    boolean isMultiPoint = true;
    MrOttDecode manager = null;
    int mrInterval = 11;
    int mrMaxSize = 1000;

    //是否合并邻区
//    boolean isMergeNCell = false;

    private int local = 0;
    private BaseCoordinateTransInt coordTrans;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration cfg = context.getConfiguration();

        xdr2MrTimeOffset = cfg.getInt("xdr2MrTimeOffset", 0);
        imsi2MrJoins.setXdr2MrTimeOffset(xdr2MrTimeOffset);

        mrInterval = cfg.getInt("mrInterval", 30);
        mrMaxSize = cfg.getInt("mrMaxSize", 150);

        String strClass = (String)cfg.get("ott_classname");
        if (strClass==null || strClass.equals("")) {
            LOG.info("LocatorReducer Error,none ottclassname found!");
            return;
        }
        try {
            Class onwClass = Class.forName(strClass);
            manager = (MrOttDecode)onwClass.newInstance();
            manager.init(cfg);
            LOG.info("LocatorReducer MrOttDecode Init sucess!");
        } catch (Exception e) {
            LOG.info("LocatorReducer MrOttDecode Exception:" + e.getMessage());
            return;
        }

        local = cfg.getInt("local", 0);
        if (local != -1) {
            String GdCorrectFile = cfg.get("GdCorrectFile");
            String BdCorrectFile = cfg.get("BdCorrectFile");
            String CorrectFile = cfg.get("CorrectFile");
            if (local == 0) {
                coordTrans = TransformCoordinate.getBaseCoordinateTransHdfs(GdCorrectFile, BdCorrectFile, CorrectFile);
            } else {
                coordTrans = TransformCoordinate.getBaseCoordinateTransFile(GdCorrectFile, BdCorrectFile, CorrectFile);
            }
        }
    }

    @Override
    protected void reduce(LocatorCombinedKeyMr key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        ottMap.clear();
//        ottList.clear();
        xdrArr[0] = null;
        xdrArr[1] = null;
        mrPointList.clear();
        imsi2MrJoins.setXdr2MrTimeOffset(xdr2MrTimeOffset);
        //迭代
        for (Text value:values) {
            //提取groupTag
            deTag(value);
            if (groupTag.equals("ott")) {
                //转化为ott对象
                OttDecode ott = new OttDecode();
                manager.stringTo(strParam,ott);
                //保存到mmeXdrMap中,先判断mmeXdr合法
                if(ott.getStartTime()!=-1&&ott.getEndTime()!=-1&&ott.getImsi()!=null&&!"".equals(ott.getImsi())){
                    context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_OTT_RECORDS).increment(1);
//                    ottMap.put(ott.getStartTime(), ott);
//                    ottList.add(ott);
                    if (xdrArr[0] == null) {
                        xdrArr[0] = ott;
                        xdrArr[1] = null;
                    } else if (xdrArr[1] == null) {
                        xdrArr[1] = ott;
                    } else {
                        xdrArr[0] = xdrArr[1];
                        xdrArr[1] = ott;
                    }
                    if (xdrArr[0] != null && xdrArr[1] != null && !mrPointList.isEmpty()) {
                        // TODO mr ott 关联
                        imsi2MrJoins.attachToMrPointList(mrPointList, xdrArr, coordTrans, context);
                        // TODO 输出
                        outputResult(context, mrPointList);
                        // 并启动下一个List
                        mrPointList.clear();
                    }
                }
            }
            if (groupTag.equals("mr")) {
                //转化为MrPoint对象
                MrPoint mrPoint = new MrPoint();
                mrPoint.stringTo(strParam);
                context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_MR_COMBINE_RECORDS).increment(1);
                mrPointList.add(mrPoint);
            }
        }
        //在循环体之外
        //对最后形成（未经过isComplete判断的）的mrPointList进行处理
        if (!mrPointList.isEmpty()) {
            if (xdrArr[0] != null) {
                // TODO mr ott 关联
                imsi2MrJoins.attachToMrPointList(mrPointList, xdrArr, coordTrans, context);
            } else {
                context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_mrottNoMatchCount).increment(mrPointList.size());
            }
            // TODO 输出
            outputResult(context, mrPointList);
            // 并启动下一个List
            mrPointList.clear();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        mrPointList.clear();
//        ottMap.clear();
//        ottList.clear();
    }

    public void deTag(Text value){
        String strValue = value.toString();
        tagedStrParams = strValue.split("\t");
        groupTag = tagedStrParams[0];
        strParam = tagedStrParams[1];
    }

    public void outputResult(Context context, List<MrPoint> mrPointList) {
        for (MrPoint mr : mrPointList) {
            try {
                if (mr.isNormalXY()) {
                    txtMr.set(mr.toString());
                    context.write(outkey, txtMr);
                }
                /*if (mr.getCoordinateLB() != null && mr.getXYZ() != null) {
                    String val = mr.getImsi() + "," + mr.getTimeStamp() + "," + mr.getScCell().getEci() + ", mrScCell:" +  mr.getScCell().getLB().getLongtitude() + "," + mr.getScCell().getLB().getLatitude() + ", ott:" + mr.getCoordinateLB().getLongtitude() + "," + mr.getCoordinateLB().getLatitude();
                    txtMr.set(val);
                    context.write(outkey, txtMr);
                }*/
            }catch (Exception e){
                LOG.info("reduce output:"+e.getMessage());
            }
        }

    }
}
