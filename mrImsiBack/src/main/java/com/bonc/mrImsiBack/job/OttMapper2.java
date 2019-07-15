package com.bonc.mrImsiBack.job;

import cn.com.bonc.main.TransformCoordinate;
import cn.com.bonc.util.inter.BaseCoordinateTransInt;
import com.bonc.decodeMrXdr.MrOttDecode;
import com.bonc.decodeMrXdr.entity.LocatorCombinedKeyMr;
import com.bonc.decodeMrXdr.entity.OttDecode;
import com.bonc.mrImsiBack.enums.LOG_PROCESSOR_COUNTER;
import mrLocateV2.coordinate.CoordinateLB;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OttMapper2 extends Mapper<LongWritable, Text, LocatorCombinedKeyMr, Text> {
    private static final Logger LOG = 	 LoggerFactory.getLogger(OttMapper2.class);
    //从文件读出的字符串
    String inputString = null;
    MrOttDecode manager = null;
    String groupTag = "ott";
    //Value
    Text txtMmeXdrParams = new Text();

    private int local = 0;
    private BaseCoordinateTransInt coordTrans;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
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
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        inputString = value.toString();
        OttDecode xdr = manager.getOtt(inputString);
        if (xdr != null) {
            if (xdr.getLon()!=0.0d && xdr.getLat()!=0.0d) {
                if (xdr.getLocateSystem() != 0) {
                    String from = getTransTag(xdr.getLocateSystem());
                    double[] a = coordTrans.getCoordinate(from, "Gps", xdr.getLon(), xdr.getLat());
                    if (a[0]!=0.0d && a[1]!=0.0d) {
                        context.getCounter(LOG_PROCESSOR_COUNTER.COORDTRANS_SUCCESS_RECORES).increment(1);
                        double lon = Double.parseDouble(String.format("%.6f", a[0]));
                        double lat = Double.parseDouble(String.format("%.6f", a[1]));
                        xdr.setLon(lon);
                        xdr.setLat(lat);
                    } else {
                        context.getCounter(LOG_PROCESSOR_COUNTER.COORDTRANS_ERR_RECORES).increment(1);
                        context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_OTT_WRONG_RECORDS).increment(1);
                        return;
                    }
                }
                //生成value
                generateOutput(xdr);
                //map输出
                context.write(manager.getGroupKey(xdr), txtMmeXdrParams);
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

    private String getTransTag(int systemCode) {
        switch (systemCode) {
            case 0:
                return "Gps";
            case 1:
                return "Bd";
            case 2:
                return "Gd";
            default:
                return "Gps";
        }
    }
}
