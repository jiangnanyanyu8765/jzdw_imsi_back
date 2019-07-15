package com.bonc.mrImsiBack.utils;

import java.util.Calendar;
import java.util.Date;

public class DateUtils {

	public static Calendar getDateByArgs(String str_date, String str_date_hour){
		Calendar calendar = Calendar.getInstance();

		int year = Integer.valueOf(str_date.substring(0, 4));
		int month = Integer.valueOf(str_date.substring(4, 6));
		int date = Integer.valueOf(str_date.substring(6, 8));
		int hourOfDay = Integer.valueOf(str_date_hour);
		calendar.set(year, month-1, date, hourOfDay, 0, 0);

		return calendar;
	}
	
	
	public static String getElapsedTime(long ms){
		long time = ms/1000;
		return time/3600 + ":" + time/60%60 + ":" + time%60;
	}
	
	public static void main(String[] args) {
		Date time = getDateByArgs("20180921", "13").getTime();
		System.out.println(time);
		System.out.println(getElapsedTime(new Date().getTime() - time.getTime()));
	}
}
