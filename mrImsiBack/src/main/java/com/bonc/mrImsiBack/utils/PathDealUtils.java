package com.bonc.mrImsiBack.utils;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class PathDealUtils {
    /**
     * 配置xdr数据时间
     * 默认配置eg: mr数据为10点-11点的数据 则xdr 时间范围应为 0940-1100
     * @param conf
     * @param str_date
     * @param str_date_hour
     */
    public static void setXdrTimeLimit(Configuration conf, String str_date, String str_date_hour) {
        long mrTime = getDateByArgs(str_date, str_date_hour).getTime().getTime();
        int advanceMinute = conf.getInt("advanceMinute", 20) == 0 ? 20 : conf.getInt("advanceMinute", 20);
        int laterMinute = 60 + conf.getInt("laterMinute", 0);
        conf.setLong("xdr_start_time", (mrTime - 1000*60*advanceMinute));
        conf.setLong("xdr_end_time", (mrTime + 1000*60*laterMinute));
        System.out.println("xdr_start_time\t" + conf.getLong("xdr_start_time", 0));
        System.out.println("xdr_end_time\t" + conf.getLong("xdr_end_time", 0));
    }

    /**
     * 山西
     * @param xdrInput
     * @param str_date
     * @param str_date_hour
     * @param xdrinputlength
     * @param xdrinputArray
     * @return
     */
    public static String[] getXdrInputPath(String xdrInput, String str_date, String str_date_hour, int xdrinputlength, String[] xdrinputArray) {
        //当前小时
        xdrinputArray[0] = xdrInput + str_date + str_date_hour;
        if(xdrinputlength == 1){
            return xdrinputArray;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
        Calendar calendar = getDateByArgs(str_date, str_date_hour);
        //前一个小时 xdrinputArray[1]
        if(xdrinputlength == 2){
            calendar.add(Calendar.HOUR, -1);
            xdrinputArray[1] = xdrInput + sdf.format(calendar.getTime());
            return xdrinputArray;
        }

        //后一个小时 xdrinputArray[2]
        if(xdrinputlength == 3){
            calendar.add(Calendar.HOUR, -1);
            xdrinputArray[1] = xdrInput + sdf.format(calendar.getTime());
            calendar.add(Calendar.HOUR, 2);
            xdrinputArray[2] = xdrInput + sdf.format(calendar.getTime());
            return xdrinputArray;
        }
        return null;
    }

    private static Calendar getDateByArgs(String str_date, String str_date_hour) {
        Calendar calendar = Calendar.getInstance();
        int year = Integer.valueOf(str_date.substring(0, 4));
        int month = Integer.valueOf(str_date.substring(4, 6));
        int date = Integer.valueOf(str_date.substring(6, 8));
        int hourOfDay = Integer.valueOf(str_date_hour);
        calendar.set(year, month-1, date, hourOfDay, 0, 0);
        return calendar;
    }

    /**
     * 贵州(标准)
     * @param xdrInput
     * @param str_date
     * @param str_date_hour
     * @param xdrinputlength
     * @param xdrinputArray
     * @return
     */
    public static String[] getXdrInputPathGuizhou(String xdrInput, String str_date, String str_date_hour, int xdrinputlength, String[] xdrinputArray) {
        //当前小时
        xdrinputArray[0] = xdrInput + File.separator + str_date + File.separator + str_date_hour;
        if(xdrinputlength == 1){
            return xdrinputArray;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
        Calendar calendar = getDateByArgs(str_date, str_date_hour);
        //前一个小时 xdrinputArray[1]
        if(xdrinputlength == 2){
            calendar.add(Calendar.HOUR, -1);
            String dayHour = sdf.format(calendar.getTime());
            xdrinputArray[1] = xdrInput + File.separator + dayHour.substring(0,8) + File.separator + dayHour.substring(8,10);
            return xdrinputArray;
        }

        //后一个小时 xdrinputArray[2]
        if(xdrinputlength == 3){
            calendar.add(Calendar.HOUR, -1);
            String dayHour = sdf.format(calendar.getTime());
            xdrinputArray[1] = xdrInput + File.separator + dayHour.substring(0,8) + File.separator + dayHour.substring(8,10);
            calendar.add(Calendar.HOUR, 2);
            dayHour = sdf.format(calendar.getTime());
            xdrinputArray[2] = xdrInput + File.separator + dayHour.substring(0,8) + File.separator + dayHour.substring(8,10);
            return xdrinputArray;
        }
        return null;
    }
}
