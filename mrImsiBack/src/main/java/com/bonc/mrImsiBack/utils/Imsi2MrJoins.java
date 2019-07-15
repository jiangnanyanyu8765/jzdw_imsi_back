package com.bonc.mrImsiBack.utils;

import java.util.List;
import java.util.Map;

import cn.com.bonc.util.inter.BaseCoordinateTransInt;
import com.bonc.decodeMrXdr.entity.OttDecode;
import com.bonc.decodeMrXdr.entity.S1MMEXdr;
import com.bonc.mrImsiBack.enums.LOG_PROCESSOR_COUNTER;
import mrLocateV2.coordinate.CoordinateLB;
import mrLocateV2.mrdata.MrPoint;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Imsi回填MrPointList,mrPoint以List形成出现。对一次通话MrPoint集合（List）只执行一次操作可提高效率。
 *
 * @author Administrator
 */
public class Imsi2MrJoins {
    private long xdr2MrTimeOffset = 0;// xdr相对mr数据的时间偏差，默认为0，如果为正值则代表xdr时间相对mr时间较晚，反之较早。
    private long xdr2MrInterVal = 30;

    public void setXdr2MrTimeOffset(int xdr2MrTimeOffset) {
        this.xdr2MrTimeOffset = xdr2MrTimeOffset;
    }

    public long getXdr2MrInterVal() {
        return xdr2MrInterVal;
    }

    public void setXdr2MrInterVal(long xdr2MrInterVal) {
        this.xdr2MrInterVal = xdr2MrInterVal;
    }

    /**
     * 得到mrPointList的起始时间，如果整个列表没有有效时间则返回-1
     *
     * @param mrPointList 待处理的mrPoint列表
     * @return startTime, 如果没有得到有效时间则为-1
     */
    private long getStartTime(List<MrPoint> mrPointList) {
        // mrPointList起始时间，初始为-1
        long startTime = -1;
        for (MrPoint mrPoint : mrPointList) {
            startTime = mrPoint.getTimeStamp();
            if (startTime != -1)
                break;
        }
        return startTime;
    }

    /**
     * 将imsi标签打到mrPointList的每个mrPoint中
     *
     * @param mrPointList 待处理的mrPoint列表
     * @param imsi        待处理的imsi
     */
    public void tagImsi2MrPointList(List<MrPoint> mrPointList, String imsi) {
        for (MrPoint mrPoint : mrPointList) {
            mrPoint.setImsi(imsi);
        }
    }

    public boolean attachImsiToMrPointList(List<MrPoint> mrPointList, Map<Long, S1MMEXdr> mmeXdrMap) {
        // 得到如果mmeXdrMap为空，则返回false
        if (mmeXdrMap.isEmpty())
            return false;
        // 得到mrPointList的开始时间
        long mrStartTime = getStartTime(mrPointList);
        // 遍历xdr，根据时间距离最近原则找到最优xdr，并获取imsi
        String imsi = getCommonImsi(mmeXdrMap, mrStartTime);
        if (imsi != null) {
            tagImsi2MrPointList(mrPointList, imsi);
            return true;
        }
        // 返回未找到
        return false;
    }

    private String getCommonImsi(Map<Long, S1MMEXdr> mmeXdrMap, long time) {
        // TODO Auto-generated method stub
        String imsi = null;
        int interval = Integer.MAX_VALUE;
        long matchedTime = -1;// 初始化为-1
        for (Map.Entry<Long, S1MMEXdr> entry : mmeXdrMap.entrySet()) {
            S1MMEXdr mmeXdr = entry.getValue();
            long mmeXdrTime = entry.getKey();
            long difftime = Math.abs(mmeXdrTime - time - xdr2MrTimeOffset * 1000);
            // 如果时间偏差小于前一个时间偏差，则赋值imsi，否则直接退出
            if (difftime < xdr2MrInterVal * 1000) {
                if (matchedTime == -1 || difftime < matchedTime) {
                    matchedTime = difftime;
                    imsi = mmeXdr.getImsi();
                }
            }

        }
        // 返回imsi
        return imsi;
    }

    public boolean attachImsiToMrPointList(List<MrPoint> mrPointList, List<S1MMEXdr> mmeXdrList) {
        // 得到如果mmeXdrList为空，则返回false
        if (mmeXdrList.isEmpty())
            return false;
        // 得到mrPointList的开始时间
        long mrStartTime = getStartTime(mrPointList);
        // 遍历xdr，根据时间距离最近原则找到最优xdr，并获取imsi
        String imsi = getCommonImsi(mmeXdrList, mrStartTime);
        if (imsi != null) {
            tagImsi2MrPointList(mrPointList, imsi);
            return true;
        }
        // 返回未找到
        return false;
    }

    private String getCommonImsi(List<S1MMEXdr> mmeXdrList, long time) {
        // TODO Auto-generated method stub
        String imsi = null;
//        int interval = Integer.MAX_VALUE;
        long matchedTime = -1;// 初始化为-1
        for (S1MMEXdr mmeXdr : mmeXdrList) {
            long mmeXdrTime = mmeXdr.getStartTime();
            long difftime = Math.abs(mmeXdrTime - time - xdr2MrTimeOffset * 1000);
            // 如果时间偏差小于前一个时间偏差，则赋值imsi，否则直接退出
            if (difftime < xdr2MrInterVal * 1000) {
                if (matchedTime == -1 || difftime < matchedTime) {
                    matchedTime = difftime;
                    imsi = mmeXdr.getImsi();
                } else {
                    break;
                }
            }
        }
        // 返回imsi
        return imsi;
    }

    public void attachImsiToMrPointList(List<MrPoint> mrPointList, S1MMEXdr[] xdrArr, Reducer.Context context) {
        String imsi = "";
        long difftime = Long.MAX_VALUE;
        for (MrPoint mr : mrPointList) {
            long mrStartTime = mr.getTimeStamp();
            if (mrStartTime == -1) {
                continue;
            }
            long xdrBeforeTime = xdrArr[0].getStartTime();
            long xdrAfterTime = Long.MAX_VALUE;
            if (xdrArr[1] != null) {
                xdrAfterTime = xdrArr[1].getStartTime();
            }
            long diff1 = Math.abs(xdrBeforeTime - mrStartTime - xdr2MrTimeOffset * 1000);
            long diff2 = Math.abs(xdrAfterTime - mrStartTime - xdr2MrTimeOffset * 1000);
            imsi = diff1 < diff2 ? xdrArr[0].getImsi() : xdrArr[1].getImsi();
            difftime = diff1 < diff2 ? diff1 : diff2;
            // 如果时间偏差小于前一个时间偏差，则赋值imsi，否则直接退出
            if (difftime < xdr2MrInterVal * 1000) {
                mr.setImsi(imsi);
                context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_imsiMatchCount).increment(1);
            } else {
                context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_imsiNoMatchCount).increment(1);
            }
        }
    }

    public boolean attachImsiToMrPointList2(List<MrPoint> mrPointList, List<S1MMEXdr> mmeXdrList) {
        String imsi = null;
        long matchedTime = -1;// 初始化为-1
        for (MrPoint mr : mrPointList) {
            long mrStartTime = mr.getTimeStamp();
            if (mrStartTime == -1) {
                continue;
            }
            for (S1MMEXdr xdr : mmeXdrList) {
                long diff = Math.abs(xdr.getStartTime() - mrStartTime - xdr2MrTimeOffset * 1000);
                if (diff < xdr2MrInterVal * 1000) {
                    if (matchedTime == -1 || diff < matchedTime) {
                        matchedTime = diff;
                        imsi = xdr.getImsi();
                    } else {
                        break;
                    }
                }
            }
        }
        if (imsi != null) {
            tagImsi2MrPointList(mrPointList, imsi);
            return true;
        }
        // 返回未找到
        return false;
    }

//    /**
//     * 为MrPointList通过查表添加imsi信息，仅为list的第一个MrPoint对象赋值。因为对于一个mrPointList来说，其IMSI只可能有一个值。
//     *
//     * @mrPointList 需要打imsi标签的mrPointList
//     * @mmeXdrMap 存储imsi信息的mmeXdrMap
//     */
//    public boolean attachImsi2MrPointList(List<MrPoint> mrPointList, Map<Long, OttDecode> ottMap, BaseCoordinateTransInt coordTrans) {
//        //得到如果mmeXdrMap为空，则返回false
//        if (ottMap.isEmpty()) return false;
//        //得到mrPointList的开始时间
//        long mrStartTime = getStartTime(mrPointList);
//        //遍历xdr，根据时间距离最近原则找到最优xdr，并获取imsi
//        String lonLat = getLonLat(ottMap, mrStartTime, coordTrans);
//        if (lonLat != null) {
//            tagLonlat2MrPointList(mrPointList, lonLat);
//            return true;
//        }
//        //返回未找到
//        return false;
//    }

//    /**
//     * 天津
//     * 从Ott集合根据时间匹配程度(时间上最近)找到经纬度
//     *
//     * @param ottMap ott集合（同一imsi,hour）
//     * @return lon, lat, 如果没有得到有效
//     */
//    private String getLonLat(Map<Long, OttDecode> ottMap, long time, BaseCoordinateTransInt coordTrans) {
//        String lon_lat = null;
////        int interval = Integer.MAX_VALUE;
//        long matchedTime = -1;//初始化为-1
//        for (Map.Entry<Long, OttDecode> entry : ottMap.entrySet()) {
//            OttDecode ott = entry.getValue();
//            long ottTime = entry.getKey();
//            long difftime = Math.abs(ottTime - time - xdr2MrTimeOffset * 1000);
//            //如果时间偏差小于前一个时间偏差，则赋值imsi，否则直接退出
//            if (difftime < xdr2MrInterVal * 1000) {
//                if (matchedTime == -1 || difftime < matchedTime) {
//                    matchedTime = difftime;
//                    if (ott.getLocateSystem() == 0) {
//                        lon_lat = ott.getLon() + "," + ott.getLat();
//                    } else {
//                        String from = getTransTag(ott.getLocateSystem());
//                        double[] a = coordTrans.getCoordinate(from, "Gps", ott.getLon(), ott.getLat());
//                        lon_lat = String.format("%.6f", a[0]) + "," + String.format("%.6f", a[1]);
//                    }
//                }
//            }
//        }
//        //返回imsi
//        return lon_lat;
//    }

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

    /**
     * 将lon,lat标签打到mrPointList的每个mrPoint中
     * @param mrPointList 待处理的mrPoint列表
     * @param lonLat 待处理的经纬度
     */
    public void tagLonlat2MrPointList(List<MrPoint> mrPointList, String lonLat){
        String[] arrs = lonLat.split(",");
        double lon = Double.parseDouble(arrs[0]);
        double lat = Double.parseDouble(arrs[1]);
        for(MrPoint mrPoint:mrPointList){
            CoordinateLB lb = new CoordinateLB(lon,lat);
            mrPoint.setLB(lb);
            mrPoint.setXYZ(lb.toXY(CoordinateLB.L0));
        }
    }

    /**
     * 为MrPointList通过查表添加imsi信息，仅为list的第一个MrPoint对象赋值。因为对于一个mrPointList来说，其IMSI只可能有一个值。
     *
     * @mrPointList 需要打imsi标签的mrPointList
     * @mmeXdrMap 存储imsi信息的mmeXdrMap
     */
    public boolean attachImsi2MrPointList(List<MrPoint> mrPointList, List<OttDecode> ottList, BaseCoordinateTransInt coordTrans, Reducer.Context context) {
        //得到如果mmeXdrMap为空，则返回false
        if (ottList.isEmpty()) return false;
        //得到mrPointList的开始时间
        long mrStartTime = getStartTime(mrPointList);
        //遍历xdr，根据时间距离最近原则找到最优xdr，并获取imsi
        String lonLat = getLonLat(ottList, mrStartTime, coordTrans, context);
        if (lonLat != null) {
            tagLonlat2MrPointList(mrPointList, lonLat);
            return true;
        }
        //返回未找到
        return false;
    }

    /**
     *
     * 从Ott集合根据时间匹配程度(时间上最近)找到经纬度
     *
     * @param ottList ott集合（同一imsi,hour）
     * @return lon, lat, 如果没有得到有效
     */
    private String getLonLat(List<OttDecode> ottList, long time, BaseCoordinateTransInt coordTrans, Reducer.Context context) {
        String lon_lat = null;
//        int interval = Integer.MAX_VALUE;
        long matchedTime = Long.MAX_VALUE;//初始化为-1
        for (OttDecode ott : ottList) {
            long ottTime = ott.getStartTime();
            long difftime = Math.abs(ottTime - time - xdr2MrTimeOffset * 1000);
            //如果时间偏差小于前一个时间偏差，则赋值imsi，否则直接退出
            if (difftime < xdr2MrInterVal * 1000) {
                if (difftime < matchedTime || lon_lat == null) {
                    matchedTime = difftime;
                    if (ott.getLocateSystem() == 0) {
                        lon_lat = ott.getLon() + "," + ott.getLat();
                    } else {
                        String from = getTransTag(ott.getLocateSystem());
                        double[] a = coordTrans.getCoordinate(from, "Gps", ott.getLon(), ott.getLat());
                        if (a[0]!=0.0d && a[1]!=0.0d) {
                            lon_lat = String.format("%.6f", a[0]) + "," + String.format("%.6f", a[1]);
                            context.getCounter(LOG_PROCESSOR_COUNTER.COORDTRANS_SUCCESS_RECORES).increment(1);
                        } else {
                            context.getCounter(LOG_PROCESSOR_COUNTER.COORDTRANS_ERR_RECORES).increment(1);
                        }
                    }
                } else {
                    break;
                }
            }
        }
        //返回imsi
        return lon_lat;
    }

    public void attachToMrPointList(List<MrPoint> mrPointList, OttDecode[] ottArr, BaseCoordinateTransInt coordTrans, Reducer.Context context) {
        String lon_lat = null;
        OttDecode ott = null;
        long difftime = Long.MAX_VALUE;
        for (MrPoint mr : mrPointList) {
            long mrStartTime = mr.getTimeStamp();
            if (mrStartTime == -1) {
                continue;
            }
            long xdrBeforeTime = ottArr[0].getStartTime();
            long xdrAfterTime = Long.MAX_VALUE;
            if (ottArr[1] != null) {
                xdrAfterTime = ottArr[1].getStartTime();
            }
            long diff1 = Math.abs(xdrBeforeTime - mrStartTime - xdr2MrTimeOffset * 1000);
            long diff2 = Math.abs(xdrAfterTime - mrStartTime - xdr2MrTimeOffset * 1000);

            ott = diff1 < diff2 ? ottArr[0] : ottArr[1];
            difftime = diff1 < diff2 ? diff1 : diff2;
            // 如果时间偏差小于前一个时间偏差，则赋值imsi，否则直接退出
            if (difftime < xdr2MrInterVal * 1000) {
                if (ott.getLocateSystem() == 0) {
                    context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_mrottMatchCount).increment(1);
                    CoordinateLB lb = new CoordinateLB(ott.getLon(),ott.getLat());
                    mr.setLB(lb);
                    mr.setXYZ(lb.toXY(CoordinateLB.L0));
                } else {
                    String from = getTransTag(ott.getLocateSystem());
                    double[] a = coordTrans.getCoordinate(from, "Gps", ott.getLon(), ott.getLat());
                    if (a[0]!=0.0d && a[1]!=0.0d) {
                        lon_lat = String.format("%.6f", a[0]) + "," + String.format("%.6f", a[1]);
                        context.getCounter(LOG_PROCESSOR_COUNTER.COORDTRANS_SUCCESS_RECORES).increment(1);
                        context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_mrottMatchCount).increment(1);
                        String[] arrs = lon_lat.split(",");
                        double lon = Double.parseDouble(arrs[0]);
                        double lat = Double.parseDouble(arrs[1]);
                        CoordinateLB lb = new CoordinateLB(lon,lat);
                        mr.setLB(lb);
                        mr.setXYZ(lb.toXY(CoordinateLB.L0));
                    } else {
                        context.getCounter(LOG_PROCESSOR_COUNTER.COORDTRANS_ERR_RECORES).increment(1);
                        context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_mrottNoMatchCount).increment(1);
                    }
                }
            } else {
                context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_mrottNoMatchCount).increment(1);
            }
        }
    }

    public void attachToMrPointList(List<MrPoint> mrPointList, OttDecode[] ottArr, Reducer.Context context) {
        String lon_lat = null;
        OttDecode ott = null;
        long difftime = Long.MAX_VALUE;
        for (MrPoint mr : mrPointList) {
            long mrStartTime = mr.getTimeStamp();
            if (mrStartTime == -1) {
                continue;
            }
            long xdrBeforeTime = ottArr[0].getStartTime();
            long xdrAfterTime = Long.MAX_VALUE;
            if (ottArr[1] != null) {
                xdrAfterTime = ottArr[1].getStartTime();
            }
            long diff1 = Math.abs(xdrBeforeTime - mrStartTime - xdr2MrTimeOffset * 1000);
            long diff2 = Math.abs(xdrAfterTime - mrStartTime - xdr2MrTimeOffset * 1000);

            ott = diff1 < diff2 ? ottArr[0] : ottArr[1];
            difftime = diff1 < diff2 ? diff1 : diff2;
            // 如果时间偏差小于前一个时间偏差，则赋值imsi，否则直接退出
            if (difftime < xdr2MrInterVal * 1000) {
                context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_mrottMatchCount).increment(1);
                CoordinateLB lb = new CoordinateLB(ott.getLon(),ott.getLat());
                mr.setLB(lb);
                mr.setXYZ(lb.toXY(CoordinateLB.L0));
            } else {
                context.getCounter(LOG_PROCESSOR_COUNTER.MRLOCATE_mrottNoMatchCount).increment(1);
            }
        }
    }

}
